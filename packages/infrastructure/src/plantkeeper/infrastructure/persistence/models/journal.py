"""ORM models of the Journal context."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Index, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from plantkeeper.infrastructure.persistence.base import Base
from plantkeeper.infrastructure.persistence.schemas import WRITE_JOURNAL


class JournalEntryModel(Base):
    """The ``write_journal.journal_entries`` table (append-only).

    There is no ``updated_at`` and no repository update path: a journal entry is
    written once. Event sourcing proper (an ``event_store`` stream and
    snapshots) is Phase 6; this table is the write side's own record.
    """

    __tablename__ = "journal_entries"
    __table_args__ = (
        Index("ix_journal_entries_plant_id_occurred_at", "plant_id", "occurred_at"),
        {"schema": WRITE_JOURNAL},
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    plant_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    entry_type: Mapped[str] = mapped_column(String(20), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
