"""The LLM loop, as distinct named stages:

build prompt -> call model -> parse -> normalize -> (retry once on parse
failure) -> fallback -> (second pass if confidence is low).

The Anthropic client is passed in so tests can substitute a fake.
"""

import json
import logging
import time
from dataclasses import dataclass

import anthropic

from app.config import ANTHROPIC_MODEL, CONFIDENCE_THRESHOLD

logger = logging.getLogger(__name__)

VALID_LABELS = {"vandalism", "substantive", "trivia", "unclear"}

FALLBACK = None  # defined after Classification; see bottom of module

MAX_CALL_ATTEMPTS = 3
BACKOFF_SECONDS = [1, 2, 4]


@dataclass
class Classification:
    label: str
    confidence: float
    reasoning: str
    model: str


class ModelCallError(Exception):
    """The API call failed after bounded retries."""


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


def call_model(client: anthropic.Anthropic, prompt: str) -> str:
    """One logical call with bounded retry + backoff on transient errors.

    Raises ModelCallError once retries are exhausted; the caller falls back
    rather than crashing the consumer.
    """
    last_error = None
    for attempt in range(MAX_CALL_ATTEMPTS):
        try:
            response = client.messages.create(
                model=ANTHROPIC_MODEL,
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
            # configured. Misconfiguration must not crash the consumer.
            raise ModelCallError(str(error)) from error
        except anthropic.APIStatusError as error:
            if error.status_code < 500:
                raise ModelCallError(str(error)) from error  # 4xx: retrying won't help
            last_error = error
        if attempt < MAX_CALL_ATTEMPTS - 1:
            time.sleep(BACKOFF_SECONDS[attempt])
    raise ModelCallError(str(last_error)) from last_error


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


def normalize(parsed: dict) -> Classification | None:
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
    return Classification(label, confidence, reasoning, ANTHROPIC_MODEL)


def _attempt(client: anthropic.Anthropic, prompt: str) -> Classification | None:
    """call -> parse -> normalize; None means the output was unusable."""
    text = call_model(client, prompt)
    parsed = parse_response(text)
    if parsed is None:
        return None
    return normalize(parsed)


def classify(client: anthropic.Anthropic, edit: dict) -> Classification:
    """Full loop for one edit. Never raises: falls back to `unclear`."""
    prompt = build_prompt(edit)
    try:
        result = _attempt(client, prompt)
        if result is None:
            # Retry once on parse failure with an explicit format reminder.
            result = _attempt(
                client, prompt + "\n\nReturn ONLY the JSON object, nothing else."
            )
    except ModelCallError as error:
        logger.warning("model call failed for edit %s: %s", edit.get("id"), error)
        result = None

    if result is None:
        return fallback()

    if result.confidence < CONFIDENCE_THRESHOLD:
        try:
            second = _attempt(client, build_second_pass_prompt(edit))
        except ModelCallError as error:
            logger.warning("second pass failed for edit %s: %s", edit.get("id"), error)
            second = None
        if second is not None:
            result = second  # it saw strictly more context

    return result


def fallback() -> Classification:
    return Classification(
        label="unclear",
        confidence=0.1,
        reasoning="fallback: model output could not be parsed or call failed",
        model=ANTHROPIC_MODEL,
    )
