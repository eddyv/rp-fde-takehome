"""Thin serve layer: GET /edits?label= reads classified edits from Postgres."""

import psycopg
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row

from app.config import POSTGRES_DSN

app = FastAPI(title="wiki-edits")

VALID_LABELS = {"vandalism", "substantive", "trivia", "unclear"}


@app.get("/edits")
def get_edits(
    label: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> list[dict]:
    if label is not None and label not in VALID_LABELS:
        raise HTTPException(
            status_code=400, detail=f"label must be one of {sorted(VALID_LABELS)}"
        )

    query = "SELECT * FROM edits"
    params: list = []
    if label is not None:
        query += " WHERE label = %s"
        params.append(label)
    query += " ORDER BY processed_at DESC LIMIT %s"
    params.append(limit)

    with psycopg.connect(POSTGRES_DSN, row_factory=dict_row) as conn:
        return conn.execute(query, params).fetchall()
