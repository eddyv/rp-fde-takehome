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
