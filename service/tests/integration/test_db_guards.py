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
import sqlalchemy.exc
from app import db
from app.classifier import Classification

from tests.integration.conftest import fetch_edit_row

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

    row = fetch_edit_row(pg_conn, EDIT["id"])
    assert row["status"] == "classified", "the guard must block the downgrade"
    assert row["label"] == "substantive"
    assert row["confidence"] == pytest.approx(0.91)
    assert row["model"] == "test-model"
    assert row["reasoning"] == "solid edit"


def test_success_upgrades_a_failed_row(pg_conn):
    db.upsert_failed_edit(pg_conn, EDIT, "parse_failed", "unusable output")
    seeded = fetch_edit_row(pg_conn, EDIT["id"])
    assert seeded["status"] == "failed" and seeded["label"] is None

    db.upsert_edit(
        pg_conn, EDIT, Classification("trivia", 0.4, "typo fix", "test-model")
    )

    row = fetch_edit_row(pg_conn, EDIT["id"])
    assert row["status"] == "classified", "a success must be allowed to upgrade"
    assert row["label"] == "trivia"
    assert row["model"] == "test-model"


def test_dataerror_rolls_back_so_the_connection_stays_usable(pg_conn):
    """Even under AUTOCOMMIT, SQLAlchemy tracks an implicit transaction; without
    `_execute`'s rollback, a DBAPIError leaves the connection unusable -- every
    later statement raises PendingRollbackError, a failure mode raw psycopg
    autocommit never had."""
    poison = {
        "id": "guard-2",
        "title": "X",
        "comment": "",
        "byte_delta": "lots",  # not an int: Postgres raises a DataError
    }
    good = {
        "id": "guard-3",
        "title": "Y",
        "user": "Bob",
        "comment": "",
        "byte_delta": 1,
        "event_time": "2026-07-01T00:00:00+00:00",
    }
    conn = db.connect()
    try:
        with pytest.raises(sqlalchemy.exc.DBAPIError):
            db.upsert_edit(
                conn, poison, Classification("trivia", 0.5, "n/a", "test-model")
            )

        # The same connection must still be usable after the error.
        db.upsert_edit(conn, good, Classification("trivia", 0.6, "fine", "test-model"))
    finally:
        conn.close()

    row = fetch_edit_row(pg_conn, good["id"])
    assert row is not None and row["status"] == "classified"
