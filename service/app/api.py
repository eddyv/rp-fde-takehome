"""Thin serve layer: GET /edits?label=&status= reads edits from Postgres."""

import psycopg
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row

from app.classifier import VALID_LABELS
from app.config import settings

app = FastAPI(title="wiki-edits")

VALID_STATUSES = {"classified", "failed"}


@app.get("/edits")
def get_edits(
    label: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> list[dict]:
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

    query = "SELECT * FROM edits"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY processed_at DESC LIMIT %s"
    params.append(limit)

    with psycopg.connect(settings.postgres_dsn, row_factory=dict_row) as conn:
        return conn.execute(query, params).fetchall()
