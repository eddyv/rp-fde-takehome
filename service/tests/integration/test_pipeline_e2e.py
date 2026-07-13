"""One end-to-end test against a real model served by Ollama's
Anthropic-compatible endpoint.

Ollama's Anthropic-compat handler silently drops `output_config`'s
json_schema (its OutputConfig struct only has an `Effort` field -- verified
in ollama/ollama's anthropic/anthropic.go), and since build_prompt() no
longer asks for JSON explicitly (it relies on schema enforcement), free-text
output most likely fails app.classifier's strict json.loads. So the
'classified' branch below is the happy path and the 'failed' branch (routed
to a retry/DLQ envelope) is the *expected*, source-verified outcome for
today's Ollama -- both are asserted as legitimate E2E outcomes over a real
broker + DB. A future Ollama version that honors output_config (or returns a
clean 400) is handled by the classified branch or the SystemExit skip below.
"""

import json
import uuid

import anthropic
import pytest
from app import classifier
from app.config import settings

from tests.integration.conftest import (
    OLLAMA_MODEL,
    produce,
    read_envelopes,
    run_worker_once,
)

pytestmark = [pytest.mark.integration, pytest.mark.llm]


def test_pipeline_e2e_real_ollama(pg_conn, ollama_base_url, monkeypatch):
    monkeypatch.setattr(settings, "anthropic_model", OLLAMA_MODEL)
    # Threshold 0.0: confidence is always >= 0, so the second-pass prompt
    # never fires -- exactly one model call per the plan's design.
    monkeypatch.setattr(settings, "confidence_threshold", 0.0)

    edit = {
        "id": str(uuid.uuid4()),
        "title": "Test Article",
        "user": "203.0.113.5",
        "comment": "removed all content and replaced with insults",
        "byte_delta": -500,
        "event_time": "2026-07-01T00:00:00+00:00",
        "rev_old": 1,
        "rev_new": 2,
        "server_name": "en.wikipedia.org",
    }
    produce(settings.kafka_topic, json.dumps(edit).encode())

    client = anthropic.Anthropic(
        base_url=ollama_base_url,
        api_key="ollama",
        max_retries=0,
        # CPU inference is slow; the worker's production default (60s) would
        # false-fail here.
        timeout=600.0,
    )

    try:
        message, committed = run_worker_once(client)
    except SystemExit:
        pytest.skip(
            "classify() raised ModelConfigError against Ollama (e.g. a "
            "future clean 400 on output_config) -- nothing to assert here"
        )

    assert committed is not None and committed == message.offset + 1, (
        "offset must be committed on every terminal path"
    )

    row = pg_conn.execute("SELECT * FROM edits WHERE id = %s", (edit["id"],)).fetchone()
    assert row is not None, "a row must exist for the edit id on every terminal path"

    if row["status"] == "classified":
        assert row["label"] in classifier.VALID_LABELS
        assert 0.0 <= row["confidence"] <= 1.0
        assert row["model"] == OLLAMA_MODEL
    elif row["status"] == "failed":
        dlq = read_envelopes(settings.kafka_dlq_topic, timeout=10, allow_empty=True)
        retry = read_envelopes(settings.kafka_retry_topic, timeout=10, allow_empty=True)
        envelopes = dlq + retry
        assert any(e.get("edit", {}).get("id") == edit["id"] for e in envelopes), (
            "a failed row must correspond to a retry or DLQ envelope"
        )
    else:
        pytest.fail(f"unexpected status {row['status']!r}")
