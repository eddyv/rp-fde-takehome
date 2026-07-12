"""Classifier taxonomy and parse-path tests; no network (see tests.fakes)."""

import pytest
from app.classifier import (
    ClassificationParseError,
    ModelConfigError,
    ModelUnavailableError,
    classify,
)

from tests.fakes import FakeClient, make_status_error

EDIT = {"id": "1", "title": "X", "comment": "", "byte_delta": 5}
GOOD_JSON = '{"label": "trivia", "confidence": 0.9, "reasoning": "typo fix"}'


def test_malformed_output_retries_once_then_raises_parse_error():
    client = FakeClient(
        [
            "Sure! The label is probably vandalism but here is no JSON.",
            "still {not valid json",
        ]
    )

    with pytest.raises(ClassificationParseError):
        classify(client, EDIT)

    assert len(client.calls) == 2, "expected exactly one retry after parse failure"
    assert "ONLY the JSON" in client.calls[1], "retry should tighten the format ask"


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
    assert len(client.calls) == 2


def test_missing_api_key_typeerror_is_config_error():
    client = FakeClient([TypeError("Could not resolve authentication method")])

    with pytest.raises(ModelConfigError):
        classify(client, EDIT)

    assert len(client.calls) == 1, "deterministic failure must not be retried"


def test_401_is_config_error_after_exactly_one_call():
    client = FakeClient([make_status_error(401)])

    with pytest.raises(ModelConfigError):
        classify(client, EDIT)

    assert len(client.calls) == 1, "4xx must not be retried"


def test_rate_limit_exhaustion_raises_unavailable_after_three_calls():
    client = FakeClient([make_status_error(429)] * 3)

    with pytest.raises(ModelUnavailableError):
        classify(client, EDIT)

    assert len(client.calls) == 3


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
