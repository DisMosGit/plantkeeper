"""ORM models of the Journal context.

Three tables, and the relationship between them is the point:

* ``journal_entries`` is the write side's own tabular record from Phase 2 — an
  append-only table with no update path. It is kept in step by the same
  transaction that appends to the event store, but it is not what answers a replay.
* ``event_store`` is the journal's source of truth: one row per appended event,
  keyed by ``(stream_id, version)``. That pair *is* the optimistic lock — a second
  writer that read version N cannot insert N+1 — and it is also the access path a
  stream is read by. ``global_position`` is the store's own append order across
  every stream, which is what makes ``load_all`` deterministic.
* ``journal_snapshots`` checkpoints a stream's state so a long journal does not have
  to be replayed from version 1.

The stream id is the plant id (``docs/event-sourcing.md``). ``payload`` holds the
event document exactly as the outbox does, and ``event_type`` is the class name —
the same vocabulary the Kafka ``event_name`` header uses, so a stored event and a
published one are the same document.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from plantkeeper.infrastructure.persistence.base import Base
from plantkeeper.infrastructure.persistence.schemas import WRITE_JOURNAL


class JournalEntryModel(Base):
    """The ``write_journal.journal_entries`` table (append-only).

    There is no ``updated_at`` and no repository update path: a journal entry is
    written once. The event store is the journal's source of truth (Phase 6); this
    table is the write side's own record of the same facts.
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


class EventStoreModel(Base):
    """The ``write_journal.event_store`` table: the journal's source of truth.

    ``version`` starts at 1 and is assigned by the appending transaction, so the
    unique ``(stream_id, version)`` key is both the optimistic lock and the order a
    stream replays in. ``event_id`` is unique too: appending the same event twice,
    whatever version it lands on, cannot produce two rows.
    """

    __tablename__ = "event_store"
    __table_args__ = (
        UniqueConstraint("stream_id", "version", name="uq_event_store_stream_id_version"),
        UniqueConstraint("event_id", name="uq_event_store_event_id"),
        CheckConstraint("version > 0", name="version_positive"),
        {"schema": WRITE_JOURNAL},
    )

    global_position: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    stream_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    event_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class JournalSnapshotModel(Base):
    """The ``write_journal.journal_snapshots`` table: checkpoints of a stream.

    One row per ``(stream_id, version)``, so a stream keeps its checkpoint history
    and a reader asks for the newest (``ORDER BY version DESC LIMIT 1``, which the
    primary key's index serves). ``state`` is the domain's serialised
    ``JournalState``; retention and pruning are deliberately out of scope.
    """

    __tablename__ = "journal_snapshots"
    __table_args__ = ({"schema": WRITE_JOURNAL},)

    stream_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    state: Mapped[dict[str, JsonValue]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
