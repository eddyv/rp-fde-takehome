"""Thin serve layer: GET /edits?label=&status=&cursor= reads edits from Postgres.

Results are cursor-paginated with a keyset on (processed_at DESC, id DESC);
the response is {"items": [...], "next_cursor": <opaque string or null>}.
"""

import base64
import binascii
import json
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import literal, select, tuple_

from app import db
from app.classifier import VALID_LABELS, VALID_STATUSES
from app.models import Edit

app = FastAPI(title="wiki-edits")


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

    stmt = (
        select(Edit)
        .order_by(Edit.processed_at.desc(), Edit.id.desc())
        .limit(limit + 1)  # the extra row tells us whether a next page exists
    )
    if label is not None:
        stmt = stmt.where(Edit.label == label)
    if status is not None:
        stmt = stmt.where(Edit.status == status)
    if cursor is not None:
        cursor_processed_at, cursor_id = decode_cursor(cursor)
        # id breaks processed_at ties so pages never skip or repeat rows.
        stmt = stmt.where(
            tuple_(Edit.processed_at, Edit.id)
            < tuple_(literal(cursor_processed_at), literal(cursor_id))
        )

    with db.get_engine().connect() as conn:
        rows = [dict(row) for row in conn.execute(stmt).mappings()]

    items = rows[:limit]
    next_cursor = (
        encode_cursor(items[-1]["processed_at"], items[-1]["id"])
        if len(rows) > limit
        else None
    )
    return {"items": items, "next_cursor": next_cursor}
