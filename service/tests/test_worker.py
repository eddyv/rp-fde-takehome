"""handle_message routing: per-class destinations, commit ordering, breaker."""

import base64
import json
from datetime import UTC, datetime

import psycopg
import pytest
import sqlalchemy.exc
from app import db, failures
from app.config import settings
from app.worker import handle_message

from tests.fakes import (
    FakeClient,
    FakeConn,
    FakeConsumer,
    FakeProducer,
    make_message,
    make_status_error,
)

EDIT = {"id": "1", "title": "X", "comment": "", "byte_delta": 5}
GOOD_JSON = '{"label": "vandalism", "confidence": 0.9, "reasoning": "blanked"}'


def make_fixtures(threshold: int = 25):
    log: list = []
    return (
        FakeConn(log),
        FakeConsumer(log),
        FakeProducer(log),
        failures.CircuitBreaker(threshold),
        log,
    )


def test_malformed_message_goes_to_dlq_then_commits():
    conn, consumer, producer, breaker, log = make_fixtures()
    message = make_message(b"not json")

    handle_message(FakeClient([]), conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_dlq_topic
    assert sent.value["reason"] == "malformed"
    assert sent.value["source"] == "worker"
    assert "Expecting value" in sent.value["error"], "decode error is the evidence"
    assert sent.key is None, "no edit id is known for undecodable payloads"
    assert base64.b64decode(sent.value["raw"]) == b"not json"
    assert log == [("publish", settings.kafka_dlq_topic), ("commit",)]
    assert conn.executed == [], "nothing classifiable, nothing written"


def test_json_scalar_message_is_malformed_not_a_crash():
    conn, consumer, producer, breaker, log = make_fixtures()
    message = make_message(b"5")  # valid JSON, but not an edit object

    handle_message(FakeClient([]), conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_dlq_topic
    assert sent.value["reason"] == "malformed"
    assert sent.value["error"] == "not an edit object: int"
    assert consumer.commits == 1


def test_object_without_id_is_malformed_not_a_crash():
    conn, consumer, producer, breaker, log = make_fixtures()
    message = make_message(b"{}")  # valid JSON object, but no usable edit id

    handle_message(FakeClient([]), conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_dlq_topic
    assert sent.value["reason"] == "malformed"
    assert sent.value["error"] == "not an edit object: dict"
    assert consumer.commits == 1
    assert conn.executed == []


def test_schema_mismatch_row_goes_to_dlq_as_malformed():
    # e.g. byte_delta that isn't an int: psycopg raises a DataError (not
    # OperationalError) — retrying can never help, so park + commit.
    log: list = []
    conn = FakeConn(
        log,
        fail_with=sqlalchemy.exc.DataError(
            "INSERT INTO edits ...",
            {},
            psycopg.DataError("invalid input for type integer"),
        ),
    )
    consumer, producer = FakeConsumer(log), FakeProducer(log)
    breaker = failures.CircuitBreaker(25)
    message = make_message(json.dumps(EDIT).encode())

    handle_message(FakeClient([GOOD_JSON]), conn, consumer, producer, breaker, message)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_dlq_topic
    assert sent.value["reason"] == "malformed"
    assert sent.value["source"] == "worker"
    assert "invalid input" in sent.value["error"]
    assert consumer.commits == 1


def test_transient_exhaustion_failed_row_then_retry_publish_then_commit():
    conn, consumer, producer, breaker, log = make_fixtures()
    client = FakeClient([make_status_error(429)] * 3)
    message = make_message(json.dumps(EDIT).encode())

    handle_message(client, conn, consumer, producer, breaker, message)

    [(sql, params)] = conn.executed
    assert sql is db.FAILED_UPSERT_STMT
    assert params["reasoning"].startswith("failed (transient_exhausted)")
    assert "http 429" in params["reasoning"], "the row carries the upstream error"
    [sent] = producer.sent
    assert sent.topic == settings.kafka_retry_topic
    assert sent.key == b"1", "envelope key must be the edit id"
    assert sent.value["reason"] == "transient_exhausted"
    assert sent.value["source"] == "worker"
    assert sent.value["error"] == "http 429"
    assert sent.value["attempts"] == 1
    assert sent.value["edit"] == EDIT
    # First retry is scheduled one base-backoff out, computed from now.
    delay = (
        datetime.fromisoformat(sent.value["not_before"]) - datetime.now(UTC)
    ).total_seconds()
    assert 25 <= delay <= settings.retry_backoff_base_seconds
    assert log == [("db",), ("publish", settings.kafka_retry_topic), ("commit",)]
    assert breaker.consecutive_failures == 1


def test_breaker_trips_on_nth_consecutive_transient_after_commit():
    conn, consumer, producer, breaker, log = make_fixtures(threshold=2)
    message = make_message(json.dumps(EDIT).encode())

    handle_message(
        FakeClient([make_status_error(429)] * 3),
        conn,
        consumer,
        producer,
        breaker,
        message,
    )
    with pytest.raises(SystemExit) as excinfo:
        handle_message(
            FakeClient([make_status_error(429)] * 3),
            conn,
            consumer,
            producer,
            breaker,
            message,
        )

    assert excinfo.value.code == 1, "must read as a failure to the restart policy"
    assert consumer.commits == 2, "breaker crash must happen after the commit"
    assert len(producer.sent) == 2, "the tripping message still reaches the retry topic"


def test_config_error_crashes_without_commit_or_publish():
    conn, consumer, producer, breaker, log = make_fixtures()
    client = FakeClient([make_status_error(401)])
    message = make_message(json.dumps(EDIT).encode())

    with pytest.raises(SystemExit) as excinfo:
        handle_message(client, conn, consumer, producer, breaker, message)

    assert excinfo.value.code == 1, "must read as a failure to the restart policy"
    assert consumer.commits == 0, "the offset must stay put for redelivery"
    assert producer.sent == []
    assert conn.executed == []


def test_parse_failure_failed_row_then_dlq_and_breaker_reset():
    conn, consumer, producer, breaker, log = make_fixtures()
    breaker.record_failure()  # pre-existing transient streak
    client = FakeClient(["no json"])
    message = make_message(json.dumps(EDIT).encode())

    handle_message(client, conn, consumer, producer, breaker, message)

    [(sql, params)] = conn.executed
    assert params["reasoning"].startswith("failed (parse_failed)")
    assert "unusable" in params["reasoning"], "the row carries the actual error"
    [sent] = producer.sent
    assert sent.topic == settings.kafka_dlq_topic
    assert sent.key == b"1", "envelope key must be the edit id"
    assert sent.value["reason"] == "parse_failed"
    assert sent.value["source"] == "worker"
    assert sent.value["attempts"] == 1
    assert "unusable" in sent.value["error"]
    assert "not_before" not in sent.value, "DLQ envelopes carry no schedule"
    assert log == [("db",), ("publish", settings.kafka_dlq_topic), ("commit",)]
    assert breaker.consecutive_failures == 0, "parse failure proves the API is up"


def test_already_classified_redelivery_skips_the_llm_and_commits():
    log: list = []
    conn = FakeConn(log, statuses={"1": "classified"})
    consumer, producer = FakeConsumer(log), FakeProducer(log)
    breaker = failures.CircuitBreaker(25)
    client = FakeClient([])  # any classify call would blow up the fake
    message = make_message(json.dumps(EDIT).encode())

    handle_message(client, conn, consumer, producer, breaker, message)

    assert client.calls == [], "an already-classified id must not re-burn the LLM"
    assert conn.status_reads == ["1"]
    assert conn.executed == [], "the durable row must stay untouched"
    assert producer.sent == []
    assert log == [("commit",)], "the offset must still be committed"


def test_failed_status_redelivery_still_reclassifies():
    log: list = []
    conn = FakeConn(log, statuses={"1": "failed"})
    consumer, producer = FakeConsumer(log), FakeProducer(log)
    breaker = failures.CircuitBreaker(25)
    client = FakeClient([GOOD_JSON])
    message = make_message(json.dumps(EDIT).encode())

    handle_message(client, conn, consumer, producer, breaker, message)

    assert len(client.calls) == 1, "a 'failed' row must reclassify, never skip"
    [(sql, params)] = conn.executed
    assert params["status"] == "classified"
    assert log == [("db",), ("commit",)]


def test_success_upserts_classified_and_resets_breaker():
    conn, consumer, producer, breaker, log = make_fixtures()
    breaker.record_failure()
    client = FakeClient([GOOD_JSON])
    message = make_message(json.dumps(EDIT).encode())

    handle_message(client, conn, consumer, producer, breaker, message)

    [(sql, params)] = conn.executed
    assert params["status"] == "classified"
    assert params["label"] == "vandalism"
    assert producer.sent == []
    assert log == [("db",), ("commit",)]
    assert breaker.consecutive_failures == 0
