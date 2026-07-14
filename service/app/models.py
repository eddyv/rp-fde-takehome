"""Mirrors sql/schema.sql, which remains the DDL source of truth -- this model
is never used for create_all; CHECK constraints and indexes are initdb
concerns and deliberately omitted."""

from datetime import datetime

from sqlalchemy import REAL, Integer, Text, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# The label/status columns' allowed values. `status` is enforced by a CHECK
# constraint in sql/schema.sql; `label` has no DB-level constraint (only a
# descriptive comment there) but this is the set the classifier is allowed
# to assign, so callers validate against it here instead.
VALID_LABELS = {"vandalism", "substantive", "trivia", "unclear"}
VALID_STATUSES = {"classified", "failed"}


class Base(DeclarativeBase):
    pass


class Edit(Base):
    __tablename__ = "edits"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    title: Mapped[str | None] = mapped_column(Text)
    editor: Mapped[str | None] = mapped_column(Text)
    comment: Mapped[str | None] = mapped_column(Text)
    byte_delta: Mapped[int | None] = mapped_column(Integer)
    label: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float | None] = mapped_column(REAL)
    reasoning: Mapped[str | None] = mapped_column(Text)
    model: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="classified"
    )
    event_time: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    processed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
