"""Thin serve layer: GET /edits?label=&status=&cursor= reads edits from Postgres.

Results are cursor-paginated with a keyset on (processed_at DESC, id DESC);
the response is {"items": [...], "next_cursor": <opaque string or null>}.
"""

import base64
import binascii
import json
from datetime import datetime

import psycopg
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row

from app.classifier import VALID_LABELS
from app.config import settings

app = FastAPI(title="wiki-edits")

VALID_STATUSES = {"classified", "failed"}


def encode_cursor(processed_at: datetime, edit_id: str) -> str:
    payload = {"p": processed_at.isoformat(), "i": edit_id}
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return datetime.fromisoformat(payload["p"]), payload["i"]
    except (binascii.Error, ValueError, KeyError, TypeError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="invalid cursor") from None


@app.get("/edits")
def get_edits(
    label: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    cursor: str | None = Query(default=None),
) -> dict:
    if label is not None and label not in VALID_LABELS:
        raise HTTPException(
            status_code=400, detail=f"label must be one of {sorted(VALID_LABELS)}"
        )
    if status is not None and status not in VALID_STATUSES:
        raise HTTPException(
            status_code=400, detail=f"status must be one of {sorted(VALID_STATUSES)}"
        )

    conditions: list[str] = []
    params: list = []
    if label is not None:
        conditions.append("label = %s")
        params.append(label)
    if status is not None:
        conditions.append("status = %s")
        params.append(status)
    if cursor is not None:
        cursor_processed_at, cursor_id = decode_cursor(cursor)
        conditions.append("(processed_at, id) < (%s, %s)")
        params.extend([cursor_processed_at, cursor_id])

    query = "SELECT * FROM edits"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    # id breaks processed_at ties so pages never skip or repeat rows.
    query += " ORDER BY processed_at DESC, id DESC LIMIT %s"
    params.append(limit + 1)  # the extra row tells us whether a next page exists

    with psycopg.connect(settings.postgres_dsn, row_factory=dict_row) as conn:
        rows = conn.execute(query, params).fetchall()

    items = rows[:limit]
    next_cursor = (
        encode_cursor(items[-1]["processed_at"], items[-1]["id"])
        if len(rows) > limit
        else None
    )
    return {"items": items, "next_cursor": next_cursor}
