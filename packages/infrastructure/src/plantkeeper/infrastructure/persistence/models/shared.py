"""The tables that serve the whole write side.

They live in ``write_shared`` rather than in a context schema because none of them
belongs to a bounded context: the outbox carries every context's events, the
idempotency keys are an HTTP concern, the saga tables belong to the process
managers, and the processed-events ledger is every Kafka consumer's.

The outbox shape follows ``docs/adr/0003-write-side-outbox.md``. Two details are
worth reading twice:

* ``event_id`` is unique, so appending the same event twice cannot produce two
  Kafka messages;
* the partial index on ``id`` covers the relay's only query — "unpublished,
  dead-letter-free rows, oldest first" — without indexing the rows that are done.

The saga tables follow ``docs/adr/0005-orchestration-vs-choreography.md``: an
execution row (status, context, optimistic-lock version) and an append-only log of
step transitions. The engine reconstructs a crashed saga's progress from the log,
so its ordering ``(created_at, id)`` is load-bearing.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from plantkeeper.infrastructure.persistence.base import Base
from plantkeeper.infrastructure.persistence.schemas import WRITE_SHARED


class OutboxModel(Base):
    """The ``write_shared.outbox`` table."""

    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_unpublished",
            "id",
            postgresql_where=text("published_at IS NULL AND dead_lettered_at IS NULL"),
        ),
        {"schema": WRITE_SHARED},
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    event_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False, unique=True)
    event_name: Mapped[str] = mapped_column(String(255), nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    partition_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dead_lettered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    attempts: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class IdempotencyKeyModel(Base):
    """The ``write_shared.idempotency_keys`` table.

    ``key`` is the primary key on purpose: the winning insert makes a concurrent
    duplicate fail, and the loser then reads the winner's stored response
    instead of overwriting it.
    """

    __tablename__ = "idempotency_keys"
    __table_args__ = (
        Index("ix_idempotency_keys_created_at", "created_at"),
        {"schema": WRITE_SHARED},
    )

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)
    response: Mapped[dict[str, JsonValue]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class SagaStateModel(Base):
    """The ``write_shared.saga_state`` table: one row per saga execution.

    ``status`` is stored as text rather than as a Postgres enum, the same choice
    the notification and light-requirement columns make: a new saga status must
    not require an ``ALTER TYPE``, and the engine's Python enum is the authority.
    ``version`` is the optimistic-locking token the storage increments on every
    write; ``recovery_attempts`` bounds how often a broken saga is retried.
    """

    __tablename__ = "saga_state"
    __table_args__ = (
        Index("ix_saga_state_status_updated_at", "name", "status", "updated_at"),
        {"schema": WRITE_SHARED},
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    context: Mapped[dict[str, JsonValue]] = mapped_column(JSONB, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    recovery_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class SagaLogModel(Base):
    """The ``write_shared.saga_log`` table: the saga's step transitions.

    Append-only. The engine reads it to decide which steps already ran and which
    already compensated, so the ``(created_at, id)`` index is what its ordering
    relies on.
    """

    __tablename__ = "saga_log"
    __table_args__ = (
        Index("ix_saga_log_saga_id_created_at", "saga_id", "created_at", "id"),
        {"schema": WRITE_SHARED},
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    saga_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("write_shared.saga_state.id"), nullable=False
    )
    step_name: Mapped[str] = mapped_column(String(255), nullable=False)
    action: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    details: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ProcessedEventModel(Base):
    """The ``write_shared.processed_events`` table: the consumer ledger.

    ``AGENTS.md`` requires every Kafka consumer to be idempotent on
    ``(consumer_group, event_id)``. The unique constraint is that rule, enforced
    by the database: the second delivery of an event in the same group cannot
    insert, so ``claim`` answers ``False`` and the handler is skipped.
    """

    __tablename__ = "processed_events"
    __table_args__ = (
        UniqueConstraint(
            "consumer_group", "event_id", name="uq_processed_events_consumer_group_event_id"
        ),
        Index("ix_processed_events_event_id", "event_id"),
        {"schema": WRITE_SHARED},
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    consumer_group: Mapped[str] = mapped_column(String(100), nullable=False)
    event_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
