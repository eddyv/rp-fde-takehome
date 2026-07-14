"""Row shape and reconnect semantics for the Postgres layer."""

import psycopg
import sqlalchemy.exc
from app import db
from app.classifier import Classification
from sqlalchemy.dialects import postgresql

from tests.fakes import FakeConn

EDIT = {
    "id": 42,
    "title": "Anarchism",
    "user": "203.0.113.9",
    "comment": "fix typo",
    "byte_delta": -3,
    "event_time": "2026-07-01T00:00:00+00:00",
}
RESULT = Classification("trivia", 0.9, "typo fix", "claude-haiku-4-5")


def test_upsert_edit_maps_every_column():
    conn = FakeConn()

    db.upsert_edit(conn, EDIT, RESULT)

    [(sql, params)] = conn.executed
    assert sql is db.UPSERT_STMT
    assert params == {
        "id": "42",  # coerced to str: Postgres column is text, ids arrive as ints
        "title": "Anarchism",
        "editor": "203.0.113.9",
        "comment": "fix typo",
        "byte_delta": -3,
        "label": "trivia",
        "confidence": 0.9,
        "reasoning": "typo fix",
        "model": "claude-haiku-4-5",
        "status": "classified",
        "event_time": "2026-07-01T00:00:00+00:00",
    }


def test_upsert_failed_edit_maps_columns_and_truncates_reasoning():
    conn = FakeConn()
    long_error = "x" * 600

    db.upsert_failed_edit(conn, EDIT, "transient_exhausted", long_error)

    [(sql, params)] = conn.executed
    assert sql is db.FAILED_UPSERT_STMT
    assert params["id"] == "42"
    assert params["title"] == "Anarchism"
    assert params["editor"] == "203.0.113.9"
    assert params["comment"] == "fix typo"
    assert params["byte_delta"] == -3
    assert params["event_time"] == "2026-07-01T00:00:00+00:00"
    assert params["reasoning"].startswith("failed (transient_exhausted): xxx")
    assert len(params["reasoning"]) == 500, (
        "the column is unbounded TEXT; the cap bounds operator-facing provenance"
    )


def test_upsert_stmt_compiles_to_the_expected_sql():
    compiled = str(db.UPSERT_STMT.compile(dialect=postgresql.dialect()))

    assert (
        "INSERT INTO edits (id, title, editor, comment, byte_delta, label, "
        "confidence, reasoning, model, status, event_time, processed_at)" in compiled
    )
    assert "processed_at) VALUES (" in compiled
    assert compiled.count("now()") == 2, "one for the insert, one for the update"
    assert "ON CONFLICT (id) DO UPDATE SET" in compiled
    assert "label = excluded.label" in compiled
    assert "confidence = excluded.confidence" in compiled
    assert "reasoning = excluded.reasoning" in compiled
    assert "model = excluded.model" in compiled
    assert "status = excluded.status" in compiled
    assert "processed_at = now()" in compiled


def test_failed_upsert_stmt_compiles_to_the_expected_sql():
    compiled = str(db.FAILED_UPSERT_STMT.compile(dialect=postgresql.dialect()))

    assert (
        "INSERT INTO edits (id, title, editor, comment, byte_delta, label, "
        "confidence, reasoning, model, status, event_time, processed_at)" in compiled
    )
    assert "VALUES (%(id)s, %(title)s, %(editor)s, %(comment)s," in compiled
    assert "%(byte_delta)s, NULL, NULL, %(reasoning)s, NULL, 'failed'" in compiled
    assert "ON CONFLICT (id) DO UPDATE SET" in compiled
    assert "label = NULL" in compiled
    assert "confidence = NULL" in compiled
    assert "reasoning = excluded.reasoning" in compiled
    assert "model = NULL" in compiled
    assert "status = 'failed'" in compiled
    assert "processed_at = now()" in compiled
    assert "WHERE edits.status IS DISTINCT FROM 'classified'" in compiled


def test_fetch_edit_status_returns_row_status_or_none_when_absent():
    conn = FakeConn(statuses={"42": "classified"})

    assert db.fetch_edit_status(conn, EDIT) == "classified"
    assert db.fetch_edit_status(conn, {"id": "unseen"}) is None
    assert conn.status_reads == ["42", "unseen"]


def test_write_with_reconnect_returns_same_conn_on_success():
    conn = FakeConn()

    returned = db.write_with_reconnect(conn, lambda c: c.execute("SELECT 1"))

    assert returned is conn
    assert [sql for sql, _ in conn.executed] == ["SELECT 1"]


def test_write_with_reconnect_retries_once_on_fresh_connection(monkeypatch):
    dead = FakeConn(
        fail_with=sqlalchemy.exc.OperationalError(
            "stmt", {}, psycopg.OperationalError("server closed")
        )
    )
    fresh = FakeConn()
    monkeypatch.setattr(db, "connect", lambda: fresh)

    returned = db.write_with_reconnect(dead, lambda c: c.execute("SELECT 1"))

    assert returned is fresh, "caller must keep using the reconnected conn"
    assert [sql for sql, _ in fresh.executed] == ["SELECT 1"]


def test_write_with_reconnect_swallows_a_failing_close_on_the_broken_conn(monkeypatch):
    dead = FakeConn(
        fail_with=sqlalchemy.exc.OperationalError(
            "stmt", {}, psycopg.OperationalError("server closed")
        ),
        fail_close_with=RuntimeError("already gone"),
    )
    fresh = FakeConn()
    monkeypatch.setattr(db, "connect", lambda: fresh)

    # The best-effort close on the broken conn must not blow up the reconnect,
    # and the write must still land on the fresh connection.
    returned = db.write_with_reconnect(dead, lambda c: c.execute("SELECT 1"))

    assert dead.closed, "close must still be attempted"
    assert returned is fresh
    assert [sql for sql, _ in fresh.executed] == ["SELECT 1"]


def test_read_with_reconnect_returns_same_conn_and_result_on_success():
    conn = FakeConn(statuses={"42": "failed"})

    returned, status = db.read_with_reconnect(
        conn, lambda c: db.fetch_edit_status(c, EDIT)
    )

    assert returned is conn
    assert status == "failed"


def test_read_with_reconnect_retries_once_on_fresh_connection(monkeypatch):
    dead = FakeConn(
        fail_with=sqlalchemy.exc.OperationalError(
            "stmt", {}, psycopg.OperationalError("server closed")
        )
    )
    fresh = FakeConn()
    monkeypatch.setattr(db, "connect", lambda: fresh)

    returned, _ = db.read_with_reconnect(dead, lambda c: c.execute("SELECT 1"))

    assert returned is fresh, "caller must keep using the reconnected conn"
    assert [sql for sql, _ in fresh.executed] == ["SELECT 1"]


def test_read_with_reconnect_swallows_a_failing_close_on_the_broken_conn(monkeypatch):
    dead = FakeConn(
        fail_with=sqlalchemy.exc.OperationalError(
            "stmt", {}, psycopg.OperationalError("server closed")
        ),
        fail_close_with=RuntimeError("already gone"),
    )
    fresh = FakeConn()
    monkeypatch.setattr(db, "connect", lambda: fresh)

    returned, _ = db.read_with_reconnect(dead, lambda c: c.execute("SELECT 1"))

    assert dead.closed, "close must still be attempted"
    assert returned is fresh
    assert [sql for sql, _ in fresh.executed] == ["SELECT 1"]
