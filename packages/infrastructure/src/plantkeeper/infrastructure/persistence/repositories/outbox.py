"""SQLAlchemy outbox repository.

The relay polls this table with its own session, never with the request-scoped
session of a unit of work: reading rows the write side has not committed yet
would break the atomicity the outbox exists to provide.
"""

from __future__ import annotations

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.application.ports.outbox import OutboxMessage
from plantkeeper.domain.base import DomainEvent
from plantkeeper.infrastructure.persistence.mappers.outbox import (
    outbox_message_from_model,
    outbox_model_from_event,
)
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel


class SqlAlchemyOutboxRepository:
    """The ``OutboxRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(self, event: DomainEvent) -> None:
        """Stage a row for ``event`` in the caller's transaction."""
        self._session.add(outbox_model_from_event(event))

    async def fetch_unpublished(self, limit: int) -> list[OutboxMessage]:
        """Return the oldest rows that still need publishing."""
        statement = (
            select(OutboxModel)
            .where(
                OutboxModel.published_at.is_(None),
                OutboxModel.dead_lettered_at.is_(None),
            )
            .order_by(OutboxModel.id)
            .limit(limit)
        )
        models = (await self._session.execute(statement)).scalars()
        return [outbox_message_from_model(model) for model in models]

    async def mark_published(self, outbox_id: int) -> None:
        """Record a successful publish."""
        await self._session.execute(
            update(OutboxModel)
            .where(OutboxModel.id == outbox_id)
            .values(published_at=func.now(), last_error=None)
        )

    async def record_failure(self, outbox_id: int, error: str) -> None:
        """Count one failed attempt and remember the reason."""
        await self._session.execute(
            update(OutboxModel)
            .where(OutboxModel.id == outbox_id)
            .values(attempts=OutboxModel.attempts + 1, last_error=error)
        )

    async def dead_letter(self, outbox_id: int, error: str) -> None:
        """Stop retrying a row that was copied to the dead-letter topic."""
        await self._session.execute(
            update(OutboxModel)
            .where(OutboxModel.id == outbox_id)
            .values(dead_lettered_at=func.now(), last_error=error)
        )
