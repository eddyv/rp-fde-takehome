"""Sweeper drain over a real Redpanda + Postgres.

`app.sweeper` is the most Kafka-semantics-dependent code in the service --
end_offsets snapshot, pause() at the boundary, explicit per-message offset
commits, the consumer_timeout_ms drain, and the requeue-to-tail loop
prevention -- yet elsewhere it is only exercised with fakes. This drives
`sweeper.main()` against a per-test DLQ seeded with a mixed batch and asserts
the broker/DB invariants that in-process doubles cannot prove:

- a reclassifiable envelope becomes a `classified` row (with the sweep model);
- a still-failing envelope is requeued to the DLQ *tail*, past the snapshot
  boundary, with attempts+1 and `first_failed_at` preserved -- and is NOT
  reprocessed in the same run (loop prevention);
- malformed / undecodable records are skipped and never produce a row;
- the sweeper group's committed offset lands exactly on the snapshot boundary,
  leaving the requeued tail uncommitted for the next sweep.

Fault injection reuses tests.fakes: `main()` builds its own anthropic client,
so we monkeypatch the constructor to return a FakeClient scripted in DLQ
offset order (one partition => consumption order is production order). Note
the blast radius: sweeper.py does `import anthropic`, so patching
`sweeper.anthropic.Anthropic` patches the shared `anthropic` module for every
importer until monkeypatch teardown -- fine while tests run serially and
nothing else builds a client mid-test, but not sweeper-scoped.
"""

import json
import sys

import pytest
from app import classifier, failures, sweeper
from app.config import settings

from tests.fakes import FakeClient, make_message, make_status_error
from tests.integration.conftest import (
    committed_offset,
    end_offset,
    produce,
    read_records,
    seed_envelope,
)

pytestmark = pytest.mark.integration

GOOD_JSON = '{"label": "substantive", "confidence": 0.8, "reasoning": "fact"}'
FIRST_FAILED_AT = "2026-07-01T00:00:00+00:00"

RECLASSIFY_EDIT = {
    "id": "sweep-reclassify",
    "title": "Reclassify Me",
    "user": "Bob",
    "comment": "restore removed content",
    "byte_delta": 128,
    "event_time": "2026-07-01T00:00:00+00:00",
}
STILL_FAILING_EDIT = {
    "id": "sweep-stillfail",
    "title": "Still Failing",
    "user": "10.0.0.1",
    "comment": "",
    "byte_delta": -9,
    "event_time": "2026-07-02T00:00:00+00:00",
}


def test_sweeper_reclassifies_requeues_skips_and_stops_at_boundary(pg_conn, monkeypatch):
    # Seed the DLQ in a deterministic offset order (topic has 1 partition, and
    # each produce() is broker-acked before the next):
    #   0: reclassifiable (parse_failed edit)     -> classify -> classified row
    #   1: still-failing (parse_failed edit)      -> classify raises -> requeued
    #   2: malformed (reason=malformed, base64 raw) -> skipped, no classify
    #   3: undecodable raw bytes                    -> skipped, no classify
    seed_envelope(
        settings.kafka_dlq_topic,
        RECLASSIFY_EDIT,
        reason=failures.REASON_PARSE_FAILED,
        error="unusable model output",
        attempts=1,
        first_failed_at=FIRST_FAILED_AT,
    )
    seed_envelope(
        settings.kafka_dlq_topic,
        STILL_FAILING_EDIT,
        reason=failures.REASON_PARSE_FAILED,
        error="unusable model output",
        attempts=1,
        first_failed_at=FIRST_FAILED_AT,
    )
    malformed = failures.make_envelope(
        reason=failures.REASON_MALFORMED,
        error="undecodable upstream bytes",
        source="test",
        message=make_message(b"", topic=settings.kafka_dlq_topic),
        raw=b"\x00\x01\x02 raw upstream bytes",
    )
    produce(settings.kafka_dlq_topic, json.dumps(malformed).encode())
    produce(settings.kafka_dlq_topic, b"this is not valid json {{{")

    boundary = end_offset(settings.kafka_dlq_topic)
    assert boundary == 4, "sanity: four seeded DLQ records before the sweep"

    # main() constructs its own anthropic.Anthropic; hand it a FakeClient with
    # outputs scripted in DLQ offset order for the two records that reach
    # classify():
    #   - reclassifiable -> GOOD_JSON (one call; confidence 0.8 >= threshold so
    #     the second-pass prompt never fires);
    #   - still-failing  -> three 500s, exhausting the classifier's bounded
    #     retry into a ModelUnavailableError.
    # Scripting EXACTLY these four outputs is also the loop-prevention proof: if
    # the requeued tail were reprocessed within this same run, classify() would
    # pop from an empty list and raise IndexError, failing the test.
    fake = FakeClient(
        [
            GOOD_JSON,
            make_status_error(500),
            make_status_error(500),
            make_status_error(500),
        ]
    )
    monkeypatch.setattr(sweeper.anthropic, "Anthropic", lambda **kwargs: fake)
    monkeypatch.setattr(sys, "argv", ["sweeper"])

    # Prove the --model/sweeper_model plumbing is real, not a no-op: set a
    # distinct sweep model (the FakeClient ignores the model arg, and classify()
    # threads it straight through to the persisted row) so the model assertion
    # below would fail if the override were dropped and the default model used.
    monkeypatch.setattr(settings, "sweeper_model", "sweep-test-model")

    # Trim avoidable dead time without touching production code: the sweeper's
    # drain-complete idle wait (CONSUMER_TIMEOUT_MS, read at make_consumer()
    # call time) and the classifier's inter-retry backoff between the three
    # scripted 500s dominate this test's wall clock otherwise.
    monkeypatch.setattr(sweeper, "CONSUMER_TIMEOUT_MS", 2000)
    monkeypatch.setattr(classifier, "BACKOFF_SECONDS", [0, 0, 0])

    sweeper.main()

    # 1) The reclassifiable edit is now a full classified row, tagged with the
    #    sweep model override (proving --model/sweeper_model actually selects
    #    the classifier model, not the default).
    row = pg_conn.execute(
        "SELECT * FROM edits WHERE id = %s", (RECLASSIFY_EDIT["id"],)
    ).fetchone()
    assert row is not None and row["status"] == "classified"
    assert row["label"] == "substantive"
    assert row["model"] == "sweep-test-model"
    assert row["editor"] == RECLASSIFY_EDIT["user"]
    assert row["byte_delta"] == RECLASSIFY_EDIT["byte_delta"]

    # 2) The still-failing edit was requeued, never persisted -- the sweeper
    #    writes a row only on success. And no skipped/malformed record wrote a
    #    row either: exactly one row exists in total.
    assert (
        pg_conn.execute(
            "SELECT * FROM edits WHERE id = %s", (STILL_FAILING_EDIT["id"],)
        ).fetchone()
        is None
    )
    assert pg_conn.execute("SELECT count(*) AS n FROM edits").fetchone()["n"] == 1

    # 3) The group committed exactly up to the snapshot boundary; the requeued
    #    tail (offset == boundary) stays uncommitted for the next sweep.
    committed = committed_offset(
        settings.sweeper_consumer_group, settings.kafka_dlq_topic
    )
    assert committed == boundary

    # 4) The requeue landed past the boundary with attempts incremented, the
    #    reason flipped to transient_exhausted (a ModelUnavailableError), and
    #    first_failed_at survived the republish.
    records = read_records(settings.kafka_dlq_topic, expected_count=boundary + 1)
    tail = [record for record in records if record.offset >= boundary]
    assert len(tail) == 1, "exactly one requeued envelope past the snapshot boundary"
    requeued = json.loads(tail[0].value)
    assert requeued["edit"]["id"] == STILL_FAILING_EDIT["id"]
    assert requeued["reason"] == failures.REASON_TRANSIENT_EXHAUSTED
    assert requeued["attempts"] == 2
    assert requeued["first_failed_at"] == FIRST_FAILED_AT
    assert requeued["source"] == "sweeper"
