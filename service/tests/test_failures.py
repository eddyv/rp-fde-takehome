"""Circuit breaker, backoff schedule, envelope shape, and acked publish."""

import base64
from datetime import datetime

from app import failures

from tests.fakes import FakeProducer, make_message


def test_breaker_trips_at_threshold_and_resets_on_success():
    breaker = failures.CircuitBreaker(threshold=3)

    assert breaker.record_failure() is False
    assert breaker.record_failure() is False
    assert breaker.record_failure() is True

    breaker.record_success()
    assert breaker.consecutive_failures == 0
    assert breaker.record_failure() is False, "success must restart the streak"


def test_retry_delay_doubles_then_caps():
    assert [failures.retry_delay_seconds(n) for n in (1, 2, 3, 4, 5)] == [
        30,
        60,
        120,
        120,
        120,
    ]


def test_next_not_before_is_delay_seconds_in_the_future():
    before = failures.utcnow()
    target = datetime.fromisoformat(failures.next_not_before(1))
    assert 29 <= (target - before).total_seconds() <= 31


def test_malformed_envelope_carries_base64_raw_and_provenance():
    message = make_message(b"not json", topic="wiki.edits.raw", offset=42)

    envelope = failures.make_envelope(
        reason=failures.REASON_MALFORMED,
        error="boom",
        source="worker",
        message=message,
        raw=message.value,
    )

    assert envelope["schema"] == 1
    assert envelope["reason"] == "malformed"
    assert envelope["error"] == "boom"
    assert envelope["source"] == "worker"
    assert envelope["attempts"] == 1, "first failure defaults to one attempt"
    assert base64.b64decode(envelope["raw"]) == b"not json"
    assert envelope["kafka"] == {
        "topic": "wiki.edits.raw",
        "partition": 0,
        "offset": 42,
    }
    assert envelope["first_failed_at"] == envelope["last_failed_at"]
    # Timestamps must be real, parseable instants stamped at build time.
    age = failures.utcnow() - datetime.fromisoformat(envelope["last_failed_at"])
    assert 0 <= age.total_seconds() < 5
    assert "not_before" not in envelope
    assert "edit" not in envelope


def test_retry_envelope_preserves_first_failed_at_across_republish():
    message = make_message(b"{}")

    envelope = failures.make_envelope(
        reason=failures.REASON_TRANSIENT_EXHAUSTED,
        error="e",
        source="retrier",
        message=message,
        edit={"id": "9"},
        attempts=2,
        first_failed_at="2026-01-01T00:00:00+00:00",
        not_before="2026-01-01T00:01:00+00:00",
    )

    assert envelope["attempts"] == 2
    assert envelope["first_failed_at"] == "2026-01-01T00:00:00+00:00"
    assert envelope["last_failed_at"] != envelope["first_failed_at"]
    assert envelope["not_before"] == "2026-01-01T00:01:00+00:00"
    assert "raw" not in envelope


def test_dlq_envelope_has_no_not_before():
    message = make_message(b"{}")

    envelope = failures.make_envelope(
        reason=failures.REASON_PARSE_FAILED,
        error="e",
        source="worker",
        message=message,
        edit={"id": "9"},
    )

    assert "not_before" not in envelope
    assert "raw" not in envelope


def test_publish_keys_by_edit_id_and_waits_for_the_ack():
    producer = FakeProducer()

    failures.publish(producer, "wiki.edits.retry", {"edit": {"id": 9}})

    [sent] = producer.sent
    assert sent.key == b"9", "numeric ids still key the partition"
    assert sent.future.get_timeout == 30, "the ack wait must be bounded"


def test_publish_without_edit_id_sends_unkeyed():
    producer = FakeProducer()

    failures.publish(producer, "wiki.edits.dlq", {"reason": "malformed"})
    failures.publish(producer, "wiki.edits.dlq", {"edit": {"title": "no id"}})

    assert [sent.key for sent in producer.sent] == [None, None]


def test_park_malformed_publishes_evidence_then_commits():
    from types import SimpleNamespace

    producer = FakeProducer()
    commits: list = []
    consumer = SimpleNamespace(commit=lambda: commits.append(True))
    message = make_message(b"\x00binary junk", offset=13)

    failures.park_malformed(
        producer, consumer, message, ValueError("boom"), source="retrier"
    )

    [sent] = producer.sent
    assert sent.value["reason"] == "malformed"
    assert sent.value["error"] == "boom"
    assert sent.value["source"] == "retrier"
    assert base64.b64decode(sent.value["raw"]) == b"\x00binary junk"
    assert commits == [True]
