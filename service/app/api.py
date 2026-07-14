"""Thin serve layer: GET /edits?label=&status=&size=&cursor= and GET /stats
read edits from Postgres.

Results are cursor-paginated by fastapi-pagination (sqlakeyset) using a
keyset on (processed_at DESC, id DESC); the response is a CursorPage:
{"items": [...], "total": null, "current_page": <cursor|null>,
"current_page_backwards": <cursor|null>, "previous_page": <cursor|null>,
"next_page": <cursor|null>}.
"""

from datetime import datetime

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi_pagination import add_pagination
from fastapi_pagination.cursor import CursorPage
from fastapi_pagination.customization import (
    CustomizedPage,
    UseIncludeTotal,
    UseParamsFields,
)
from fastapi_pagination.ext.sqlalchemy import paginate
from pydantic import BaseModel, ConfigDict
from sqlakeyset import InvalidPage
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app import db
from app.classifier import VALID_LABELS, VALID_STATUSES
from app.models import Edit

app = FastAPI(title="wiki-edits")


class EditOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    title: str | None
    editor: str | None
    comment: str | None
    byte_delta: int | None
    label: str | None
    confidence: float | None
    reasoning: str | None
    model: str | None
    status: str
    event_time: datetime | None
    processed_at: datetime


EditsPage = CustomizedPage[
    CursorPage[EditOut],
    UseParamsFields(size=Query(50, ge=1, le=500)),
    UseIncludeTotal(False),  # no COUNT(*) per page
]


def get_session():
    with Session(db.get_engine()) as session:
        yield session


def _validate_choice(value: str | None, valid: set[str], field: str) -> None:
    if value is not None and value not in valid:
        raise HTTPException(
            status_code=400, detail=f"{field} must be one of {sorted(valid)}"
        )


@app.get("/edits")
def get_edits(
    label: str | None = Query(default=None),
    status: str | None = Query(default=None),
    session: Session = Depends(get_session),
) -> EditsPage:
    _validate_choice(label, VALID_LABELS, "label")
    _validate_choice(status, VALID_STATUSES, "status")

    query = select(Edit).order_by(Edit.processed_at.desc(), Edit.id.desc())
    if label is not None:
        query = query.where(Edit.label == label)
    if status is not None:
        query = query.where(Edit.status == status)
    try:
        return paginate(session, query)
    except InvalidPage:
        raise HTTPException(status_code=400, detail="invalid cursor") from None


STATS_STMT = select(Edit.label, Edit.status, func.count()).group_by(
    Edit.label, Edit.status
)


def summarize_stats(rows) -> dict:
    """Fold (label|None, status, count) rows into the /stats response.

    label is NULL on failed rows, so NULL labels count toward total and
    by_status but never appear in by_label. Zero-count keys are absent,
    not zero-filled.
    """
    total = 0
    by_label: dict[str, int] = {}
    by_status: dict[str, int] = {}
    for label, status, count in rows:
        total += count
        if label is not None:
            by_label[label] = by_label.get(label, 0) + count
        by_status[status] = by_status.get(status, 0) + count
    return {"total": total, "by_label": by_label, "by_status": by_status}


@app.get("/stats")
def get_stats(session: Session = Depends(get_session)) -> dict:
    return summarize_stats(session.execute(STATS_STMT).all())


add_pagination(app)
