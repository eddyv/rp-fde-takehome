import logging
import time
from collections.abc import Callable
from contextlib import suppress

import sqlalchemy.exc
from sqlalchemy import (
    Engine,
    bindparam,
    create_engine,
    func,
    literal_column,
    null,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection

from app.classifier import Classification
from app.config import settings
from app.models import Edit

logger = logging.getLogger(__name__)

# Connection-level failures worth reconnecting for (and, if reconnect also
# fails, crashing so Kafka redelivers) rather than parking as malformed data.
# InterfaceError is a *sibling* of OperationalError, not a subclass (verified
# via issubclass() against both sqlalchemy.exc and psycopg) — psycopg raises
# it for e.g. "the cursor is closed" after a connection has already died.
CONNECTION_ERRORS = (sqlalchemy.exc.OperationalError, sqlalchemy.exc.InterfaceError)


def normalize_dsn(dsn: str) -> str:
    """SQLAlchemy's plain postgresql:// scheme means psycopg2; we ship psycopg 3."""
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    return dsn


_engines: dict[str, Engine] = {}


def get_engine() -> Engine:
    # Keyed by DSN because integration fixtures repoint settings.postgres_dsn
    # at a per-session container; a module-level singleton would freeze the
    # first DSN seen.
    dsn = normalize_dsn(settings.postgres_dsn)
    if dsn not in _engines:
        _engines[dsn] = create_engine(
            dsn, isolation_level="AUTOCOMMIT", pool_pre_ping=True
        )
    return _engines[dsn]


# Bound columns shared by both statements below: every edit carries these
# regardless of whether classification succeeded or failed.
_COMMON_EDIT_VALUES = {
    "id": bindparam("id"),
    "title": bindparam("title"),
    "editor": bindparam("editor"),
    "comment": bindparam("comment"),
    "byte_delta": bindparam("byte_delta"),
    "reasoning": bindparam("reasoning"),
    "event_time": bindparam("event_time"),
}

_upsert = pg_insert(Edit).values(
    **_COMMON_EDIT_VALUES,
    label=bindparam("label"),
    confidence=bindparam("confidence"),
    model=bindparam("model"),
    status=bindparam("status"),
    processed_at=func.now(),
)
UPSERT_STMT = _upsert.on_conflict_do_update(
    index_elements=[Edit.id],
    set_={
        "label": _upsert.excluded.label,
        "confidence": _upsert.excluded.confidence,
        "reasoning": _upsert.excluded.reasoning,
        "model": _upsert.excluded.model,
        "status": _upsert.excluded.status,
        "processed_at": func.now(),
    },
)

# A redelivered/stale failure must never downgrade a row a later (or
# concurrent) success already classified — hence the status guard.
_failed_upsert = pg_insert(Edit).values(
    **_COMMON_EDIT_VALUES,
    label=null(),
    confidence=null(),
    model=null(),
    status=literal_column("'failed'"),
    processed_at=func.now(),
)
FAILED_UPSERT_STMT = _failed_upsert.on_conflict_do_update(
    index_elements=[Edit.id],
    set_={
        "label": null(),
        "confidence": null(),
        "reasoning": _failed_upsert.excluded.reasoning,
        "model": null(),
        "status": literal_column("'failed'"),
        "processed_at": func.now(),
    },
    # literal_column (not a Python string) is deliberate: the compiled SQL
    # must contain IS DISTINCT FROM 'classified' verbatim so the guard stays
    # inspectable text.
    where=Edit.status.is_distinct_from(literal_column("'classified'")),
)

STATUS_STMT = select(Edit.status).where(Edit.id == bindparam("id"))


def _execute(conn: Connection, statement, params: dict | None = None):
    try:
        return conn.execute(statement, params)
    except sqlalchemy.exc.DBAPIError:
        # AUTOCOMMIT still tracks an implicit transaction; without this
        # rollback the long-lived pipeline connection would raise
        # PendingRollbackError on every statement after a data error.
        conn.rollback()
        raise


def connect(retries: int = 30, delay: float = 2.0) -> Connection:
    """Postgres may still be warming up when the worker starts."""
    for attempt in range(retries):
        try:
            return get_engine().connect()
        except sqlalchemy.exc.OperationalError as error:
            if attempt == retries - 1:
                raise
            logger.info("postgres not ready (%s), retrying...", error)
            time.sleep(delay)
    raise RuntimeError("unreachable")


def _common_edit_params(edit: dict) -> dict:
    return {
        "id": str(edit["id"]),
        "title": edit.get("title"),
        "editor": edit.get("user"),
        "comment": edit.get("comment"),
        "byte_delta": edit.get("byte_delta"),
        "event_time": edit.get("event_time"),  # already ISO from Bloblang
    }


def upsert_edit(conn: Connection, edit: dict, result: Classification) -> None:
    _execute(
        conn,
        UPSERT_STMT,
        {
            **_common_edit_params(edit),
            "label": result.label,
            "confidence": result.confidence,
            "reasoning": result.reasoning,
            "model": result.model,
            "status": "classified",
        },
    )


def upsert_failed_edit(conn: Connection, edit: dict, reason: str, error: str) -> None:
    """Record failure provenance in the row without touching the label enum."""
    _execute(
        conn,
        FAILED_UPSERT_STMT,
        {
            **_common_edit_params(edit),
            # The column is unbounded TEXT; the cap only bounds the
            # operator-facing provenance an upstream error can dump into it.
            "reasoning": f"failed ({reason}): {error}"[:500],
        },
    )


def fetch_edit_status(conn: Connection, edit: dict) -> str | None:
    """Status of the edit's row, or None when no row exists yet."""
    row = _execute(conn, STATUS_STMT, {"id": str(edit["id"])}).fetchone()
    return row[0] if row is not None else None


def _reconnect(conn: Connection) -> Connection:
    logger.warning("postgres connection lost, reconnecting")
    with suppress(Exception):
        conn.close()
    return connect()


def write_with_reconnect(
    conn: Connection, write: Callable[[Connection], None]
) -> Connection:
    """Run a write, reconnecting once if Postgres dropped the connection."""
    try:
        write(conn)
    except CONNECTION_ERRORS:
        conn = _reconnect(conn)
        write(conn)
    return conn


def read_with_reconnect[T](
    conn: Connection, read: Callable[[Connection], T]
) -> tuple[Connection, T]:
    """Run a read, reconnecting once if Postgres dropped the connection."""
    try:
        return conn, read(conn)
    except CONNECTION_ERRORS:
        conn = _reconnect(conn)
        return conn, read(conn)
