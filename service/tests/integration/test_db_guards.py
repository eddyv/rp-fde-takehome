"""The never-downgrade guard lives entirely in FAILED_UPSERT_SQL's
`WHERE edits.status IS DISTINCT FROM 'classified'` clause, so only real
Postgres can prove it fires. A stale or redelivered failure write must never
clobber a row that a later (or concurrent) success already classified -- while
the reverse (a success upgrading a failed row) must still be allowed. Both
directions are asserted here at the SQL boundary; the retrier integration test
exercises the upgrade end-to-end, but only this pins the guard's asymmetry to
the query itself.
"""

import pytest
from app import db
from app.classifier import Classification

pytestmark = pytest.mark.integration

EDIT = {
    "id": "guard-1",
    "title": "Anarchism",
    "user": "Alice",
    "comment": "expand history",
    "byte_delta": 42,
    "event_time": "2026-07-01T00:00:00+00:00",
}


def test_failed_upsert_never_downgrades_a_classified_row(pg_conn):
    db.upsert_edit(
        pg_conn, EDIT, Classification("substantive", 0.91, "solid edit", "test-model")
    )

    # A stale/redelivered failure for the same id must be a no-op.
    db.upsert_failed_edit(pg_conn, EDIT, "transient_exhausted", "http 500")

    row = pg_conn.execute(
        "SELECT * FROM edits WHERE id = %s", (EDIT["id"],)
    ).fetchone()
    assert row["status"] == "classified", "the guard must block the downgrade"
    assert row["label"] == "substantive"
    assert row["confidence"] == pytest.approx(0.91)
    assert row["model"] == "test-model"
    assert row["reasoning"] == "solid edit"


def test_success_upgrades_a_failed_row(pg_conn):
    db.upsert_failed_edit(pg_conn, EDIT, "parse_failed", "unusable output")
    seeded = pg_conn.execute(
        "SELECT * FROM edits WHERE id = %s", (EDIT["id"],)
    ).fetchone()
    assert seeded["status"] == "failed" and seeded["label"] is None

    db.upsert_edit(
        pg_conn, EDIT, Classification("trivia", 0.4, "typo fix", "test-model")
    )

    row = pg_conn.execute(
        "SELECT * FROM edits WHERE id = %s", (EDIT["id"],)
    ).fetchone()
    assert row["status"] == "classified", "a success must be allowed to upgrade"
    assert row["label"] == "trivia"
    assert row["model"] == "test-model"
