"""handle_envelope: republish/promotion semantics, delay handling, breaker."""

import base64
import json
from datetime import UTC, datetime, timedelta

import app.retrier as retrier
import pytest
from app import failures
from app.config import settings
from app.retrier import handle_envelope, wait_until

from tests.fakes import (
    FakeClient,
    FakeConn,
    FakeConsumer,
    FakeProducer,
    make_message,
    make_status_error,
)

EDIT = {"id": "9", "title": "Y", "comment": "", "byte_delta": -40}
GOOD_JSON = '{"label": "substantive", "confidence": 0.8, "reasoning": "fact"}'
FIRST_FAILED = "2020-01-01T00:00:00+00:00"
PAST = "2020-01-01T00:00:30+00:00"


def make_fixtures(threshold: int = 25):
    log: list = []
    return (
        FakeConn(log),
        FakeConsumer(log),
        FakeProducer(log),
        failures.CircuitBreaker(threshold),
        log,
    )


def make_envelope_message(attempts: int = 1, **overrides):
    envelope = {
        "schema": 1,
        "reason": "transient_exhausted",
        "error": "e",
        "source": "worker",
        "attempts": attempts,
        "first_failed_at": FIRST_FAILED,
        "last_failed_at": FIRST_FAILED,
        "not_before": PAST,
        "edit": EDIT,
        "kafka": {"topic": "wiki.edits.raw", "partition": 0, "offset": 1},
    }
    envelope.update(overrides)
    return make_message(json.dumps(envelope).encode(), topic=settings.kafka_retry_topic)


def test_transient_republish_increments_attempts_and_preserves_first_failed():
    conn, consumer, producer, breaker, log = make_fixtures()
    client = FakeClient([make_status_error(429)] * 3)
    message = make_envelope_message(attempts=1)

    handle_envelope(client, conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_retry_topic
    assert sent.key == b"9", "envelope key must be the edit id"
    assert sent.value["reason"] == "transient_exhausted"
    assert sent.value["error"] == "http 429"
    assert sent.value["attempts"] == 2
    assert sent.value["first_failed_at"] == FIRST_FAILED
    assert sent.value["source"] == "retrier"
    assert sent.value["edit"] == EDIT
    # New not_before is computed from now, not from the stale envelope.
    delay = (
        datetime.fromisoformat(sent.value["not_before"]) - datetime.now(UTC)
    ).total_seconds()
    assert 50 <= delay <= 61, "attempt 2 should be scheduled ~60s out"
    [(sql, params)] = conn.executed
    assert params["reasoning"].startswith("failed (transient_exhausted)")
    assert "http 429" in params["reasoning"], "the row carries the upstream error"
    assert log == [("db",), ("publish", settings.kafka_retry_topic), ("commit",)]
    assert breaker.consecutive_failures == 1


def test_exhausted_attempts_promote_to_dlq():
    conn, consumer, producer, breaker, log = make_fixtures()
    client = FakeClient([make_status_error(429)] * 3)
    message = make_envelope_message(attempts=settings.max_retry_passes)

    handle_envelope(client, conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_dlq_topic
    assert sent.key == b"9", "envelope key must be the edit id"
    assert sent.value["reason"] == "retries_exhausted"
    assert sent.value["source"] == "retrier"
    assert sent.value["error"] == "http 429"
    assert sent.value["edit"] == EDIT
    assert sent.value["attempts"] == settings.max_retry_passes + 1
    assert sent.value["first_failed_at"] == FIRST_FAILED
    assert "not_before" not in sent.value, "DLQ envelopes carry no schedule"
    assert consumer.commits == 1


def test_success_flips_row_to_classified_and_resets_breaker():
    conn, consumer, producer, breaker, log = make_fixtures()
    breaker.record_failure()
    client = FakeClient([GOOD_JSON])
    message = make_envelope_message(attempts=2)

    handle_envelope(client, conn, consumer, producer, breaker, message)

    [(sql, params)] = conn.executed
    assert params["status"] == "classified"
    assert params["label"] == "substantive"
    assert producer.sent == []
    assert consumer.commits == 1
    assert breaker.consecutive_failures == 0


def test_parse_failure_goes_to_dlq_and_resets_breaker():
    conn, consumer, producer, breaker, log = make_fixtures()
    breaker.record_failure()
    client = FakeClient(["no json"])
    message = make_envelope_message(attempts=2)

    handle_envelope(client, conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_dlq_topic
    assert sent.key == b"9", "envelope key must be the edit id"
    assert sent.value["reason"] == "parse_failed"
    assert sent.value["source"] == "retrier"
    assert sent.value["attempts"] == 2, "parse failure preserves the attempt count"
    assert "unusable" in sent.value["error"]
    assert sent.value["first_failed_at"] == FIRST_FAILED
    [(sql, params)] = conn.executed
    assert params["reasoning"].startswith("failed (parse_failed)")
    assert "unusable" in params["reasoning"], "the row carries the actual error"
    assert consumer.commits == 1
    assert breaker.consecutive_failures == 0


def test_config_error_crashes_without_commit_or_publish():
    conn, consumer, producer, breaker, log = make_fixtures()
    client = FakeClient([make_status_error(401)])
    message = make_envelope_message()

    with pytest.raises(SystemExit) as excinfo:
        handle_envelope(client, conn, consumer, producer, breaker, message)

    assert excinfo.value.code == 1, "must read as a failure to the restart policy"
    assert consumer.commits == 0
    assert producer.sent == []


def test_breaker_trips_after_commit_with_failure_exit_code():
    conn, consumer, producer, breaker, log = make_fixtures(threshold=1)
    client = FakeClient([make_status_error(429)] * 3)
    message = make_envelope_message(attempts=1)

    with pytest.raises(SystemExit) as excinfo:
        handle_envelope(client, conn, consumer, producer, breaker, message)

    assert excinfo.value.code == 1, "must read as a failure to the restart policy"
    assert consumer.commits == 1, "breaker crash must happen after the commit"
    assert len(producer.sent) == 1, "the tripping envelope still republishes"


def test_undecodable_envelope_goes_to_dlq_as_malformed():
    conn, consumer, producer, breaker, log = make_fixtures()
    message = make_message(b"garbage", topic=settings.kafka_retry_topic)

    handle_envelope(FakeClient([]), conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_dlq_topic
    assert sent.value["reason"] == "malformed"
    assert base64.b64decode(sent.value["raw"]) == b"garbage"
    assert consumer.commits == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"not_before": 123},  # fromisoformat(123) -> TypeError
        {"not_before": "not a timestamp"},  # -> ValueError
        {"attempts": "two"},  # int("two") -> ValueError
        {"attempts": {}},  # int({}) -> TypeError
        {"edit": {}},  # no usable edit id
        {"edit": "5"},  # not an object
    ],
)
def test_invalid_envelope_fields_go_to_dlq_as_malformed(overrides):
    conn, consumer, producer, breaker, log = make_fixtures()
    client = FakeClient([])  # any classify call would blow up the fake
    message = make_envelope_message(**overrides)

    handle_envelope(client, conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_dlq_topic
    assert sent.value["reason"] == "malformed"
    assert client.calls == [], "invalid envelopes must be parked before classify"
    assert consumer.commits == 1


def test_unexpected_envelope_schema_version_goes_to_dlq_as_malformed():
    conn, consumer, producer, breaker, log = make_fixtures()
    client = FakeClient([])  # any classify call would blow up the fake
    message = make_envelope_message(schema=2)

    handle_envelope(client, conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_dlq_topic
    assert sent.value["reason"] == "malformed"
    assert sent.value["error"] == "unsupported envelope schema: 2"
    assert client.calls == [], "unknown versions must be parked before classify"
    assert consumer.commits == 1


def test_numeric_string_attempts_is_coerced_not_parked():
    conn, consumer, producer, breaker, log = make_fixtures()
    client = FakeClient([make_status_error(429)] * 3)
    message = make_envelope_message(attempts="2")

    handle_envelope(client, conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.value["attempts"] == 3


def test_missing_attempts_field_defaults_to_first_attempt():
    """Envelope without `attempts` is a first attempt — processed, not parked."""
    conn, consumer, producer, breaker, log = make_fixtures()
    client = FakeClient([make_status_error(429)] * 3)
    envelope = json.loads(make_envelope_message().value)
    del envelope["attempts"]
    message = make_message(
        json.dumps(envelope).encode(), topic=settings.kafka_retry_topic
    )

    handle_envelope(client, conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_retry_topic, "must not be parked as malformed"
    assert sent.value["attempts"] == 2, "missing attempts counts as attempt 1, then +1"


def test_schema_mismatch_row_goes_to_dlq_as_malformed():
    import psycopg

    log: list = []
    conn = FakeConn(log, fail_with=psycopg.DataError("invalid input for type integer"))
    consumer, producer = FakeConsumer(log), FakeProducer(log)
    breaker = failures.CircuitBreaker(25)
    message = make_envelope_message()

    handle_envelope(FakeClient([GOOD_JSON]), conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_dlq_topic
    assert sent.value["reason"] == "malformed"
    assert sent.value["source"] == "retrier"
    assert "invalid input" in sent.value["error"]
    assert consumer.commits == 1


def test_non_object_edit_error_names_the_offending_type():
    conn, consumer, producer, breaker, log = make_fixtures()
    message = make_envelope_message(edit="5")

    handle_envelope(FakeClient([]), conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.value["error"] == "not an edit object: str"
    assert sent.value["source"] == "retrier"


def test_wait_until_past_timestamp_does_not_sleep(monkeypatch):
    sleeps: list = []
    monkeypatch.setattr(retrier.time, "sleep", lambda s: sleeps.append(s))

    wait_until(PAST)
    wait_until(None)
    wait_until("2020-01-01T00:00:00")  # naive: assumed UTC, still in the past
    wait_until(123)  # junk: retry immediately rather than wedge

    assert sleeps == []


def test_wait_until_sleeps_in_chunks_of_at_most_five_seconds(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    clock = {"now": start}
    sleeps: list = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        clock["now"] += timedelta(seconds=seconds)

    monkeypatch.setattr(retrier, "_now", lambda: clock["now"])
    monkeypatch.setattr(retrier.time, "sleep", fake_sleep)

    wait_until((start + timedelta(seconds=11)).isoformat())

    assert sleeps == [5, 5, 1], "the final sub-second-chunk must still be slept"


def test_wait_until_treats_naive_future_timestamp_as_utc(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    clock = {"now": start}
    sleeps: list = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        clock["now"] += timedelta(seconds=seconds)

    monkeypatch.setattr(retrier, "_now", lambda: clock["now"])
    monkeypatch.setattr(retrier.time, "sleep", fake_sleep)

    # No tzinfo on the timestamp: still a real schedule, not an instant pass.
    wait_until("2026-01-01T00:00:04")

    assert sleeps == [4]


def test_handle_envelope_waits_for_not_before_then_classifies(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    clock = {"now": start}
    events: list = []

    def fake_sleep(seconds):
        events.append(("sleep", seconds))
        clock["now"] += timedelta(seconds=seconds)

    monkeypatch.setattr(retrier, "_now", lambda: clock["now"])
    monkeypatch.setattr(retrier.time, "sleep", fake_sleep)

    conn, consumer, producer, breaker, log = make_fixtures()

    class EventClient(FakeClient):
        def create(self, **kwargs):
            events.append(("classify",))
            return super().create(**kwargs)

    client = EventClient([GOOD_JSON])
    message = make_envelope_message(
        not_before=(start + timedelta(seconds=7)).isoformat()
    )

    handle_envelope(client, conn, consumer, producer, breaker, message)

    assert events == [("sleep", 5), ("sleep", 2), ("classify",)], (
        "the envelope's schedule must be honored before calling the model"
    )
