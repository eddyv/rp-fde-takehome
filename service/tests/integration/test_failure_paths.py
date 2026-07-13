"""Failure-path integration tests: worker/retrier routing against real
Redpanda + Postgres. Fault injection reuses tests.fakes (a real LLM can't
reliably emit 429/5xx on demand)."""

import base64
import json
from datetime import UTC, datetime, timedelta

import pytest
from app import db
from app.config import settings

from tests.fakes import FakeClient, make_status_error
from tests.integration.conftest import (
    produce,
    read_envelopes,
    run_retrier_once,
    run_worker_once,
    seed_envelope,
)

pytestmark = pytest.mark.integration

EDIT = {"id": "1", "title": "X", "comment": "", "byte_delta": 5}
GOOD_JSON = '{"label": "substantive", "confidence": 0.8, "reasoning": "fact"}'
FULL_EDIT = {
    "id": "happy-1",
    "title": "Anarchism",
    "user": "Alice",
    "comment": "expand history section",
    "byte_delta": 320,
    "event_time": "2026-07-01T00:00:00+00:00",
}


def test_worker_happy_path_writes_full_row_and_is_idempotent(pg_conn):
    """The success path over the real broker without the LLM: every column is
    mapped (note editor <- user), the offset is committed, and a redelivery of
    the identical edit leaves exactly one row -- the UPSERT idempotency the
    worker docstring promises for at-least-once delivery."""
    produce(settings.kafka_topic, json.dumps(FULL_EDIT).encode())

    message, committed = run_worker_once(FakeClient([GOOD_JSON]))

    assert committed is not None and committed == message.offset + 1
    row = pg_conn.execute(
        "SELECT * FROM edits WHERE id = %s", (FULL_EDIT["id"],)
    ).fetchone()
    assert row["status"] == "classified"
    assert row["title"] == "Anarchism"
    assert row["editor"] == "Alice"  # edit["user"] maps to the editor column
    assert row["comment"] == "expand history section"
    assert row["byte_delta"] == 320
    assert row["label"] == "substantive"
    assert row["confidence"] == pytest.approx(0.8)
    assert row["reasoning"] == "fact"
    assert row["model"] == settings.anthropic_model
    assert row["event_time"] == datetime(2026, 7, 1, tzinfo=UTC)
    first_processed_at = row["processed_at"]

    # Redeliver the identical edit: the UPSERT must collapse it onto one row.
    produce(settings.kafka_topic, json.dumps(FULL_EDIT).encode())
    message2, committed2 = run_worker_once(FakeClient([GOOD_JSON]))

    assert committed2 is not None and committed2 == message2.offset + 1
    # The redelivery must have taken the SUCCESS path -- not been parked to the
    # DLQ. If UPSERT_SQL lost its ON CONFLICT clause (or regressed to DO
    # NOTHING), the second write would raise UniqueViolation, which the worker
    # catches as psycopg.Error and routes to park_malformed -> DLQ, leaving the
    # existing row untouched. Row count alone can't tell those worlds apart (the
    # failed INSERT writes no row either), so pin the success path directly: the
    # row's processed_at must advance, which only the ON CONFLICT DO UPDATE re-
    # running a second time can do -- while still collapsing onto exactly one row.
    row2 = pg_conn.execute(
        "SELECT * FROM edits WHERE id = %s", (FULL_EDIT["id"],)
    ).fetchone()
    assert row2["processed_at"] > first_processed_at, (
        "the UPSERT must have re-run on redelivery (success path), not been parked"
    )
    assert (
        pg_conn.execute("SELECT count(*) AS n FROM edits").fetchone()["n"] == 1
    ), "redelivery must not create a second row"


def test_schema_mismatch_parks_to_dlq_with_real_psycopg_error(pg_conn):
    """The poison-message design hinges on real Postgres raising a parkable
    psycopg.Error (a DataError, not OperationalError) for a value that does not
    fit the schema. A byte_delta that isn't an int must be parked as malformed
    -- with the original bytes -- and leave no row, proving the
    `except psycopg.Error -> park_malformed` branch fires with the real driver
    instead of wedging the partition."""
    poison = {"id": "poison-1", "title": "X", "comment": "", "byte_delta": "lots"}
    produce(settings.kafka_topic, json.dumps(poison).encode())

    client = FakeClient([GOOD_JSON])
    message, committed = run_worker_once(client)

    assert committed is not None and committed == message.offset + 1
    # Both worker park branches emit identical `malformed` envelopes; pin THIS
    # one (post-classify, at the DB write) by proving the model was consulted
    # and the error is Postgres's, not a JSON-decode failure.
    assert len(client.calls) == 1, "the edit must reach classification first"
    [envelope] = read_envelopes(settings.kafka_dlq_topic)
    assert envelope["reason"] == "malformed"
    assert "invalid input syntax" in envelope["error"]
    assert json.loads(base64.b64decode(envelope["raw"])) == poison
    assert (
        pg_conn.execute(
            "SELECT * FROM edits WHERE id = %s", (poison["id"],)
        ).fetchone()
        is None
    )


def test_transient_exhaustion_goes_to_retry_topic_and_failed_row(pg_conn):
    produce(settings.kafka_topic, json.dumps(EDIT).encode())
    client = FakeClient([make_status_error(500)] * 3)

    message, committed = run_worker_once(client)

    assert committed is not None and committed == message.offset + 1, (
        "offset must be committed on the transient-exhausted path"
    )
    row = pg_conn.execute("SELECT * FROM edits WHERE id = %s", (EDIT["id"],)).fetchone()
    assert row["status"] == "failed"

    [envelope] = read_envelopes(settings.kafka_retry_topic)
    assert envelope["reason"] == "transient_exhausted"
    assert envelope["attempts"] == 1
    not_before = datetime.fromisoformat(envelope["not_before"])
    assert not_before > datetime.now(UTC) - timedelta(seconds=5), (
        "not_before must be a real schedule computed at publish time"
    )


def test_malformed_payload_goes_to_dlq_with_raw_bytes_and_no_db_row(pg_conn):
    produce(settings.kafka_topic, b"not json {")

    message, committed = run_worker_once(FakeClient([]))

    assert committed is not None and committed == message.offset + 1
    [envelope] = read_envelopes(settings.kafka_dlq_topic)
    assert envelope["reason"] == "malformed"
    assert base64.b64decode(envelope["raw"]) == b"not json {"
    assert pg_conn.execute("SELECT * FROM edits").fetchall() == []


def test_parse_failure_goes_to_dlq_and_failed_row(pg_conn):
    produce(settings.kafka_topic, json.dumps(EDIT).encode())
    client = FakeClient(["this is not json"])

    message, committed = run_worker_once(client)

    assert committed is not None and committed == message.offset + 1
    row = pg_conn.execute("SELECT * FROM edits WHERE id = %s", (EDIT["id"],)).fetchone()
    assert row["status"] == "failed"
    [envelope] = read_envelopes(settings.kafka_dlq_topic)
    assert envelope["reason"] == "parse_failed"


def test_retrier_promotes_exhausted_retries_to_dlq(pg_conn, monkeypatch):
    monkeypatch.setattr(settings, "max_retry_passes", 1)
    past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    first_failed = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    seed_envelope(
        settings.kafka_retry_topic,
        EDIT,
        reason="transient_exhausted",
        error="previous failure",
        attempts=1,
        not_before=past,
        first_failed_at=first_failed,
    )
    client = FakeClient([make_status_error(500)] * 3)

    message, committed = run_retrier_once(client)

    assert committed is not None and committed == message.offset + 1
    row = pg_conn.execute("SELECT * FROM edits WHERE id = %s", (EDIT["id"],)).fetchone()
    assert row["status"] == "failed"

    [envelope] = read_envelopes(settings.kafka_dlq_topic)
    assert envelope["reason"] == "retries_exhausted"
    assert envelope["attempts"] == 2
    assert envelope["first_failed_at"] == first_failed, (
        "first_failed_at must survive every republish"
    )


def test_retrier_success_flips_failed_row_to_classified(pg_conn):
    db.upsert_failed_edit(pg_conn, EDIT, "transient_exhausted", "http 500")
    seeded = pg_conn.execute(
        "SELECT * FROM edits WHERE id = %s", (EDIT["id"],)
    ).fetchone()
    assert seeded["status"] == "failed", "sanity check on the seeded row"

    past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    seed_envelope(
        settings.kafka_retry_topic,
        EDIT,
        reason="transient_exhausted",
        error="previous failure",
        attempts=1,
        not_before=past,
    )
    client = FakeClient([GOOD_JSON])

    message, committed = run_retrier_once(client)

    assert committed is not None and committed == message.offset + 1
    row = pg_conn.execute("SELECT * FROM edits WHERE id = %s", (EDIT["id"],)).fetchone()
    assert row["status"] == "classified"
    assert row["label"] == "substantive"
