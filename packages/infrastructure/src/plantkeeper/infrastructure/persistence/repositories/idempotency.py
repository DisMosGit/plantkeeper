"""SQLAlchemy idempotency repository."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.application.ports.idempotency import IdempotencyRecord
from plantkeeper.infrastructure.persistence.mappers.outbox import (
    idempotency_to_domain,
    idempotency_to_model,
)
from plantkeeper.infrastructure.persistence.models.shared import IdempotencyKeyModel


class SqlAlchemyIdempotencyRepository:
    """The ``IdempotencyRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, key: str) -> IdempotencyRecord | None:
        """Return the response stored for ``key``, or ``None``."""
        model = await self._session.get(IdempotencyKeyModel, key)
        return None if model is None else idempotency_to_domain(model)

    async def remember(self, record: IdempotencyRecord) -> None:
        """Stage the record in the caller's transaction.

        Nothing is flushed here: the row must become visible together with the
        aggregate it answers for, which is exactly what the unit of work's single
        commit gives us.
        """
        self._session.add(idempotency_to_model(record))
