import logging
import time
from collections.abc import Callable

import psycopg

from app.classifier import Classification
from app.config import settings

logger = logging.getLogger(__name__)

UPSERT_SQL = """
INSERT INTO edits (id, title, editor, comment, byte_delta, label, confidence,
                   reasoning, model, status, event_time, processed_at)
VALUES (%(id)s, %(title)s, %(editor)s, %(comment)s, %(byte_delta)s, %(label)s,
        %(confidence)s, %(reasoning)s, %(model)s, %(status)s, %(event_time)s, now())
ON CONFLICT (id) DO UPDATE SET
    label = EXCLUDED.label,
    confidence = EXCLUDED.confidence,
    reasoning = EXCLUDED.reasoning,
    model = EXCLUDED.model,
    status = EXCLUDED.status,
    processed_at = now()
"""

# A redelivered/stale failure must never downgrade a row a later (or
# concurrent) success already classified — hence the status guard.
FAILED_UPSERT_SQL = """
INSERT INTO edits (id, title, editor, comment, byte_delta, label, confidence,
                   reasoning, model, status, event_time, processed_at)
VALUES (%(id)s, %(title)s, %(editor)s, %(comment)s, %(byte_delta)s, NULL,
        NULL, %(reasoning)s, NULL, 'failed', %(event_time)s, now())
ON CONFLICT (id) DO UPDATE SET
    label = NULL,
    confidence = NULL,
    reasoning = EXCLUDED.reasoning,
    model = NULL,
    status = 'failed',
    processed_at = now()
WHERE edits.status IS DISTINCT FROM 'classified'
"""

STATUS_SQL = "SELECT status FROM edits WHERE id = %(id)s"


def connect(retries: int = 30, delay: float = 2.0) -> psycopg.Connection:
    """Postgres may still be warming up when the worker starts."""
    for attempt in range(retries):
        try:
            return psycopg.connect(settings.postgres_dsn, autocommit=True)
        except psycopg.OperationalError as error:
            if attempt == retries - 1:
                raise
            logger.info("postgres not ready (%s), retrying...", error)
            time.sleep(delay)
    raise RuntimeError("unreachable")


def upsert_edit(conn: psycopg.Connection, edit: dict, result: Classification) -> None:
    conn.execute(
        UPSERT_SQL,
        {
            "id": str(edit["id"]),
            "title": edit.get("title"),
            "editor": edit.get("user"),
            "comment": edit.get("comment"),
            "byte_delta": edit.get("byte_delta"),
            "label": result.label,
            "confidence": result.confidence,
            "reasoning": result.reasoning,
            "model": result.model,
            "status": "classified",
            "event_time": edit.get("event_time"),  # already ISO from Bloblang
        },
    )


def upsert_failed_edit(
    conn: psycopg.Connection, edit: dict, reason: str, error: str
) -> None:
    """Record failure provenance in the row without touching the label enum."""
    conn.execute(
        FAILED_UPSERT_SQL,
        {
            "id": str(edit["id"]),
            "title": edit.get("title"),
            "editor": edit.get("user"),
            "comment": edit.get("comment"),
            "byte_delta": edit.get("byte_delta"),
            # The column is unbounded TEXT; the cap only bounds the
            # operator-facing provenance an upstream error can dump into it.
            "reasoning": f"failed ({reason}): {error}"[:500],
            "event_time": edit.get("event_time"),
        },
    )


def fetch_edit_status(conn: psycopg.Connection, edit: dict) -> str | None:
    """Status of the edit's row, or None when no row exists yet."""
    row = conn.execute(STATUS_SQL, {"id": str(edit["id"])}).fetchone()
    return row[0] if row is not None else None


def write_with_reconnect(
    conn: psycopg.Connection, write: Callable[[psycopg.Connection], None]
) -> psycopg.Connection:
    """Run a write, reconnecting once if Postgres dropped the connection."""
    try:
        write(conn)
    except psycopg.OperationalError:
        logger.warning("postgres connection lost, reconnecting")
        conn = connect()
        write(conn)
    return conn


def read_with_reconnect[T](
    conn: psycopg.Connection, read: Callable[[psycopg.Connection], T]
) -> tuple[psycopg.Connection, T]:
    """Run a read, reconnecting once if Postgres dropped the connection."""
    try:
        return conn, read(conn)
    except psycopg.OperationalError:
        logger.warning("postgres connection lost, reconnecting")
        conn = connect()
        return conn, read(conn)
