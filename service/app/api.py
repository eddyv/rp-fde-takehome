"""Thin serve layer: GET /edits?label=&status=&size=&cursor= reads edits from
Postgres.

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
from sqlalchemy import select
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


@app.get("/edits")
def get_edits(
    label: str | None = Query(default=None),
    status: str | None = Query(default=None),
    session: Session = Depends(get_session),
) -> EditsPage:
    if label is not None and label not in VALID_LABELS:
        raise HTTPException(
            status_code=400, detail=f"label must be one of {sorted(VALID_LABELS)}"
        )
    if status is not None and status not in VALID_STATUSES:
        raise HTTPException(
            status_code=400, detail=f"status must be one of {sorted(VALID_STATUSES)}"
        )

    query = select(Edit).order_by(Edit.processed_at.desc(), Edit.id.desc())
    if label is not None:
        query = query.where(Edit.label == label)
    if status is not None:
        query = query.where(Edit.status == status)
    try:
        return paginate(session, query)
    except InvalidPage:
        raise HTTPException(status_code=400, detail="invalid cursor") from None


add_pagination(app)
