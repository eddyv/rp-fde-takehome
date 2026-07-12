"""Classifier taxonomy and parse-path tests; no network (see tests.fakes)."""

import app.classifier
import pytest
from app.classifier import (
    VALID_LABELS,
    ClassificationParseError,
    ModelConfigError,
    ModelUnavailableError,
    build_prompt,
    build_second_pass_prompt,
    classify,
    normalize,
    parse_response,
)
from app.config import settings

from tests.fakes import FakeClient, make_block, make_status_error

EDIT = {"id": "1", "title": "X", "comment": "", "byte_delta": 5}
GOOD_JSON = '{"label": "trivia", "confidence": 0.9, "reasoning": "typo fix"}'


def test_malformed_output_retries_once_then_raises_parse_error():
    client = FakeClient(
        [
            "Sure! The label is probably vandalism but here is no JSON.",
            "still {not valid json",
        ]
    )

    # The error must name the edit so DLQ provenance stays useful.
    with pytest.raises(ClassificationParseError, match="edit 1"):
        classify(client, EDIT)

    assert len(client.calls) == 2, "expected exactly one retry after parse failure"
    assert client.calls[0] == build_prompt(EDIT)
    assert client.calls[1] == (
        build_prompt(EDIT) + "\n\nReturn ONLY the JSON object, nothing else."
    ), "retry must be the same prompt with only the format reminder appended"


def test_retry_recovers_when_second_output_is_dirty_but_parseable():
    client = FakeClient(
        [
            "no json here at all",
            'Here you go: {"label": " Vandalism ", "confidence": 0.9, '
            '"reasoning": "page blanked"} hope that helps!',
        ]
    )

    result = classify(
        client, {"id": "2", "title": "Y", "comment": "", "byte_delta": -4000}
    )

    assert result.label == "vandalism"  # extracted from prose, trimmed, lowercased
    assert result.confidence == 0.9
    assert result.model == settings.anthropic_model, "retry keeps the same model"
    assert len(client.calls) == 2


def test_missing_api_key_typeerror_is_config_error():
    client = FakeClient([TypeError("Could not resolve authentication method")])

    with pytest.raises(ModelConfigError, match="authentication"):
        classify(client, EDIT)

    assert len(client.calls) == 1, "deterministic failure must not be retried"


def test_401_is_config_error_after_exactly_one_call():
    client = FakeClient([make_status_error(401)])

    with pytest.raises(ModelConfigError, match="http 401"):
        classify(client, EDIT)

    assert len(client.calls) == 1, "4xx must not be retried"


def test_rate_limit_exhaustion_raises_unavailable_after_three_calls(monkeypatch):
    sleeps: list = []
    monkeypatch.setattr(app.classifier.time, "sleep", lambda s: sleeps.append(s))
    client = FakeClient([make_status_error(429)] * 3)

    # The message must carry the last upstream error for the failed row/DLQ.
    with pytest.raises(ModelUnavailableError, match="http 429"):
        classify(client, EDIT)

    assert len(client.calls) == 3
    assert sleeps == [1, 2], "backoff between attempts, none after the last"


def test_request_shape_is_single_user_message_with_bounded_tokens():
    client = FakeClient([GOOD_JSON])

    classify(client, EDIT)

    [kwargs] = client.kwargs
    assert kwargs["model"] == settings.anthropic_model
    assert kwargs["max_tokens"] == 256
    assert kwargs["messages"] == [{"role": "user", "content": build_prompt(EDIT)}]


def test_non_text_blocks_are_ignored_and_text_blocks_concatenated():
    client = FakeClient(
        [
            [
                make_block("thinking", "NOT JSON {{{"),
                make_block("text", '{"label": "trivia", "confidence'),
                make_block("text", '": 0.9, "reasoning": "typo fix"}'),
            ]
        ]
    )

    result = classify(client, EDIT)

    assert result.label == "trivia"
    assert result.confidence == 0.9
    assert len(client.calls) == 1, "the joined text blocks must parse first try"


@pytest.mark.parametrize("status", [500, 408, 409])
def test_transient_status_then_success_recovers(status):
    client = FakeClient([make_status_error(status), GOOD_JSON])

    result = classify(client, EDIT)

    assert result.label == "trivia"
    assert len(client.calls) == 2


def test_second_pass_transient_failure_keeps_first_result():
    low_conf = '{"label": "unclear", "confidence": 0.3, "reasoning": "meh"}'
    client = FakeClient([low_conf] + [make_status_error(429)] * 3)

    result = classify(client, EDIT)

    assert result.label == "unclear"
    assert result.confidence == 0.3
    assert len(client.calls) == 4, "first pass + 3 exhausted second-pass attempts"


def test_second_pass_config_error_still_propagates():
    low_conf = '{"label": "unclear", "confidence": 0.3, "reasoning": "meh"}'
    client = FakeClient([low_conf, make_status_error(401)])

    with pytest.raises(ModelConfigError):
        classify(client, EDIT)


def test_model_override_lands_in_classification():
    client = FakeClient([GOOD_JSON])

    result = classify(client, EDIT, model="claude-sonnet-4-5")

    assert result.model == "claude-sonnet-4-5"
    assert client.kwargs[0]["model"] == "claude-sonnet-4-5"


def test_low_confidence_triggers_second_pass_and_its_answer_wins():
    low = '{"label": "unclear", "confidence": 0.3, "reasoning": "meh"}'
    high = '{"label": "substantive", "confidence": 0.85, "reasoning": "adds facts"}'
    client = FakeClient([low, high])

    result = classify(client, EDIT)

    assert result.label == "substantive", "the second pass saw more context"
    assert result.confidence == 0.85
    assert result.reasoning == "adds facts"
    assert client.calls[1] == build_second_pass_prompt(EDIT)
    assert client.kwargs[1]["model"] == settings.anthropic_model


def test_confidence_at_threshold_skips_second_pass():
    at_threshold = (
        f'{{"label": "trivia", "confidence": {settings.confidence_threshold}, '
        '"reasoning": "ok"}'
    )
    client = FakeClient([at_threshold])

    result = classify(client, EDIT)

    assert result.confidence == settings.confidence_threshold
    assert len(client.calls) == 1, "second pass is for strictly-below only"


def test_second_pass_unusable_output_keeps_first_result():
    low = '{"label": "unclear", "confidence": 0.3, "reasoning": "meh"}'
    client = FakeClient([low, "no json here"])

    result = classify(client, EDIT)

    assert result.label == "unclear"
    assert result.confidence == 0.3
    assert len(client.calls) == 2, "second pass is single-shot, no format retry"


@pytest.mark.parametrize("status", [500, 502, 503])
def test_5xx_exhaustion_raises_unavailable_after_three_calls(status):
    client = FakeClient([make_status_error(status)] * 3)

    with pytest.raises(ModelUnavailableError, match=f"http {status}"):
        classify(client, EDIT)

    assert len(client.calls) == 3


def test_prompt_label_menu_matches_the_validation_enum():
    prompt = build_prompt(EDIT)

    for label in VALID_LABELS:
        assert f"- {label}:" in prompt, (
            "every label normalize() accepts must be offered to the model, "
            "or valid answers get rejected as enum drift"
        )


def test_prompt_demands_json_with_the_keys_parse_and_normalize_expect():
    prompt = build_prompt(EDIT)

    assert "JSON object" in prompt, "parse_response only extracts {...} objects"
    for key in ("label", "confidence", "reasoning"):
        assert f'"{key}"' in prompt, "normalize() reads exactly these keys"


def test_prompt_includes_the_edit_fields_the_model_judges():
    edit = {"id": "1", "title": "Anarchism", "comment": "fix typo", "byte_delta": -3}

    prompt = build_prompt(edit)

    assert "Anarchism" in prompt
    assert "fix typo" in prompt
    assert "-3" in prompt, "byte delta is the strongest vandalism signal"


def test_build_prompt_placeholder_for_empty_comment():
    assert "Edit comment: (none)\n" in build_prompt(EDIT)


def test_second_pass_prompt_extends_first_pass_with_editor_context():
    edit = {
        "id": "1",
        "title": "Anarchism",
        "comment": "fix typo",
        "byte_delta": -3,
        "user": "203.0.113.9",
        "rev_old": 100,
        "rev_new": 101,
        "server_name": "en.wikipedia.org",
    }

    prompt = build_second_pass_prompt(edit)

    assert prompt.startswith(build_prompt(edit)), (
        "both passes must judge the same base task, differing only in context"
    )
    extra = prompt[len(build_prompt(edit)) :]
    assert "203.0.113.9" in extra, "editor identity is the point of the second pass"
    assert "100" in extra and "101" in extra, "both revision ids give the model hints"
    assert "en.wikipedia.org" in extra


def test_parse_response_takes_first_object_and_handles_nesting():
    assert parse_response('pre {"a": {"b": 1}} post {"c": 2}') == {"a": {"b": 1}}


def test_parse_response_handles_braces_at_any_position():
    assert parse_response('{"a": 1}') == {"a": 1}
    assert parse_response('x{"a": 1}') == {"a": 1}, "object starting at index 1"
    assert parse_response('{"a": 1}x tail') == {"a": 1}, "text right after the brace"
    assert parse_response('} {"a": 1}') == {"a": 1}, "stray brace before the object"


@pytest.mark.parametrize(
    "text",
    [
        "",  # empty model output
        "no braces at all",
        "{never closes",
        '{"bad": json,}',
        "[1, 2, 3]",  # JSON but not an object
        'the answer is ["x"] ok',
    ],
)
def test_parse_response_rejects_unusable_output(text):
    assert parse_response(text) is None


def test_normalize_trims_and_lowercases_label_and_reasoning():
    result = normalize(
        {"label": " Vandalism ", "confidence": 0.7, "reasoning": "  blanked  "},
        "m",
    )

    assert result.label == "vandalism"
    assert result.confidence == 0.7
    assert result.reasoning == "blanked"
    assert result.model == "m"


def test_normalize_rejects_labels_outside_the_enum():
    assert normalize({"label": "spammy", "confidence": 0.9}, "m") is None
    assert normalize({"confidence": 0.9}, "m") is None


@pytest.mark.parametrize(
    ("raw_confidence", "expected"),
    [
        (1.5, 1.0),  # clamped down
        (-0.5, 0.0),  # clamped up
        ("0.4", 0.4),  # numeric string coerced
        ("high", 0.0),  # junk floors, never inflates
        (None, 0.0),
        ("__missing__", 0.0),  # absent key floors too
    ],
)
def test_normalize_clamps_and_floors_confidence(raw_confidence, expected):
    parsed = {"label": "trivia", "reasoning": "r"}
    if raw_confidence != "__missing__":
        parsed["confidence"] = raw_confidence

    assert normalize(parsed, "m").confidence == expected


def test_normalize_defaults_missing_reasoning_to_empty_string():
    assert normalize({"label": "trivia", "confidence": 0.9}, "m").reasoning == ""
