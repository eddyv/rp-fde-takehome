import logging
import time

import psycopg

from app.classifier import Classification
from app.config import POSTGRES_DSN

logger = logging.getLogger(__name__)

UPSERT_SQL = """
INSERT INTO edits (id, title, editor, comment, byte_delta, label, confidence,
                   reasoning, model, event_time, processed_at)
VALUES (%(id)s, %(title)s, %(editor)s, %(comment)s, %(byte_delta)s, %(label)s,
        %(confidence)s, %(reasoning)s, %(model)s, %(event_time)s, now())
ON CONFLICT (id) DO UPDATE SET
    label = EXCLUDED.label,
    confidence = EXCLUDED.confidence,
    reasoning = EXCLUDED.reasoning,
    model = EXCLUDED.model,
    processed_at = now()
"""


def connect(retries: int = 30, delay: float = 2.0) -> psycopg.Connection:
    """Postgres may still be warming up when the worker starts."""
    for attempt in range(retries):
        try:
            return psycopg.connect(POSTGRES_DSN, autocommit=True)
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
            "event_time": edit.get("event_time"),  # already ISO from Bloblang
        },
    )
