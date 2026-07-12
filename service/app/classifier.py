"""The LLM loop, as distinct named stages:

build prompt -> call model -> parse -> normalize -> (retry once on parse
failure) -> (second pass if confidence is low).

Failures surface as a typed taxonomy instead of a fallback row, so callers
can route each class differently (crash / retry topic / DLQ — see worker.py):

- ModelConfigError: deterministic (no key, 4xx) — retrying cannot help.
- ModelUnavailableError: transient errors exhausted the in-process budget.
- ClassificationParseError: output stayed unusable after the format retry.

The Anthropic client is passed in so tests can substitute a fake.
"""

import json
import logging
import time
from dataclasses import dataclass

import anthropic

from app.config import settings

logger = logging.getLogger(__name__)

VALID_LABELS = {"vandalism", "substantive", "trivia", "unclear"}

MAX_CALL_ATTEMPTS = 3
BACKOFF_SECONDS = [1, 2, 4]


@dataclass
class Classification:
    label: str
    confidence: float
    reasoning: str
    model: str


class ClassifierError(Exception):
    """Base class for the failure taxonomy below."""


class ModelConfigError(ClassifierError):
    """Deterministic failure (missing key, 4xx): fail fast, never swallow."""


class ModelUnavailableError(ClassifierError):
    """Transient errors (429/5xx/408/409/network) exhausted bounded retries."""


class ClassificationParseError(ClassifierError):
    """Model output was unusable even after the format-reminder retry."""


def build_prompt(edit: dict) -> str:
    return (
        "You are reviewing a single English Wikipedia edit. Classify it as one of:\n"
        "- vandalism: bad-faith damage (blanking, slurs, nonsense, spam)\n"
        "- substantive: good-faith change to article content or facts\n"
        "- trivia: minor housekeeping (typos, formatting, categories, punctuation)\n"
        "- unclear: not enough signal to decide\n\n"
        f"Article title: {edit.get('title')}\n"
        f"Edit comment: {edit.get('comment') or '(none)'}\n"
        f"Byte delta: {edit.get('byte_delta')}\n\n"
        'Respond with only a JSON object: {"label": "...", "confidence": 0.0-1.0, '
        '"reasoning": "one sentence"}'
    )


def build_second_pass_prompt(edit: dict) -> str:
    # More context for the low-confidence branch: editor identity and revision
    # ids give hints (anonymous IPs and large deltas correlate with vandalism).
    return (
        build_prompt(edit) + "\n\nAdditional context for a more careful judgment:\n"
        f"Editor: {edit.get('user')}\n"
        f"Revision: {edit.get('rev_old')} -> {edit.get('rev_new')}\n"
        f"Wiki host: {edit.get('server_name')}\n"
        "Weigh whether the editor looks like an anonymous IP and whether the "
        "byte delta is consistent with the edit comment."
    )


def call_model(client: anthropic.Anthropic, prompt: str, model: str) -> str:
    """One logical call with bounded retry + backoff on transient errors.

    Raises ModelConfigError immediately on deterministic failures, and
    ModelUnavailableError once the transient retry budget is exhausted.
    """
    last_error = None
    for attempt in range(MAX_CALL_ATTEMPTS):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            return "".join(
                block.text for block in response.content if block.type == "text"
            )
        except (anthropic.RateLimitError, anthropic.APIConnectionError) as error:
            last_error = error
        except TypeError as error:
            # The SDK raises TypeError at request time when no API key is
            # configured — deterministic, so crash loudly instead of retrying.
            raise ModelConfigError(str(error)) from error
        except anthropic.APIStatusError as error:
            # 408/409 are retryable per Anthropic docs; other 4xx are not.
            if error.status_code >= 500 or error.status_code in (408, 409):
                last_error = error
            else:
                raise ModelConfigError(str(error)) from error
        if attempt < MAX_CALL_ATTEMPTS - 1:
            time.sleep(BACKOFF_SECONDS[attempt])
    raise ModelUnavailableError(str(last_error)) from last_error


def parse_response(text: str) -> dict | None:
    """Extract the first {...} block from possibly dirty model output.

    Brace-counting handles nested objects; returns None if nothing parses.
    """
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    for index in range(start, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                try:
                    parsed = json.loads(text[start : index + 1])
                except json.JSONDecodeError:
                    return None
                return parsed if isinstance(parsed, dict) else None
    return None


def normalize(parsed: dict, model: str) -> Classification | None:
    """Validate the parsed object; models drift outside the enum."""
    label = str(parsed.get("label", "")).strip().lower()
    if label not in VALID_LABELS:
        return None
    try:
        confidence = float(parsed.get("confidence", 0.0))
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = min(max(confidence, 0.0), 1.0)
    reasoning = str(parsed.get("reasoning", "")).strip()
    return Classification(label, confidence, reasoning, model)


def _attempt(
    client: anthropic.Anthropic, prompt: str, model: str
) -> Classification | None:
    """call -> parse -> normalize; None means the output was unusable."""
    text = call_model(client, prompt, model)
    parsed = parse_response(text)
    if parsed is None:
        return None
    return normalize(parsed, model)


def classify(
    client: anthropic.Anthropic, edit: dict, model: str | None = None
) -> Classification:
    """Full loop for one edit. Raises the taxonomy above; never fabricates.

    `model` overrides settings.anthropic_model (used by the DLQ sweeper).
    """
    model = model or settings.anthropic_model
    prompt = build_prompt(edit)

    result = _attempt(client, prompt, model)
    if result is None:
        # Retry once on parse failure with an explicit format reminder.
        result = _attempt(
            client, prompt + "\n\nReturn ONLY the JSON object, nothing else.", model
        )
    if result is None:
        raise ClassificationParseError(
            f"model output unusable after format retry for edit {edit.get('id')}"
        )

    if result.confidence < settings.confidence_threshold:
        # Second pass is best-effort: a transient failure or unusable output
        # keeps the first-pass result. ModelConfigError still propagates —
        # misconfiguration must never be swallowed.
        try:
            second = _attempt(client, build_second_pass_prompt(edit), model)
        except ModelUnavailableError as error:
            logger.warning("second pass failed for edit %s: %s", edit.get("id"), error)
            second = None
        if second is not None:
            result = second  # it saw strictly more context

    return result
