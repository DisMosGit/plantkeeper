"""Repositories for the saga-side state.

Three small tables, one class each:

* :class:`SqlAlchemyProcessedEventRepository` — the consumer ledger. ``claim`` is
  a PostgreSQL ``INSERT … ON CONFLICT DO NOTHING``: the database decides the
  winner of two concurrent deliveries, and only the winner's transaction goes on
  to do the work.
* :class:`SqlAlchemySagaStateRepository` — read access to saga executions. The
  engine's own storage owns the writes and answers recovery with identifiers only,
  so this is where the rows themselves are read back.
* :class:`SqlAlchemyMissedCareWindowRepository` — the grace windows the
  choreography MissedCareSaga and its scheduler share.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.application.ports.sagas import (
    MissedCareState,
    MissedCareWindow,
    SagaState,
)
from plantkeeper.domain.identifiers import PlantId
from plantkeeper.infrastructure.persistence.mappers.sagas import (
    missed_care_window_to_domain,
    missed_care_window_to_model,
    saga_state_to_domain,
)
from plantkeeper.infrastructure.persistence.models.care import MissedCareWindowModel
from plantkeeper.infrastructure.persistence.models.shared import (
    ProcessedEventModel,
    SagaStateModel,
)


class SqlAlchemyProcessedEventRepository:
    """The ``ProcessedEventRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def claim(self, consumer_group: str, event_id: UUID) -> bool:
        """Insert the delivery, answering ``True`` only for the first one.

        The insert is not flushed or committed here: it belongs to the caller's
        transaction, so a handler that fails releases the claim with it.
        """
        statement = (
            insert(ProcessedEventModel)
            .values(consumer_group=consumer_group, event_id=event_id)
            .on_conflict_do_nothing(
                index_elements=[
                    ProcessedEventModel.consumer_group,
                    ProcessedEventModel.event_id,
                ]
            )
            .returning(ProcessedEventModel.id)
        )
        # ``DO NOTHING`` returns no row exactly when the delivery was a duplicate,
        # which is the answer the caller needs.
        return (await self._session.execute(statement)).scalar_one_or_none() is not None


class SqlAlchemySagaStateRepository:
    """The ``SagaStateRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, saga_id: UUID) -> SagaState | None:
        """Return the saga's row, or ``None``."""
        model = await self._session.get(SagaStateModel, saga_id)
        return None if model is None else saga_state_to_domain(model)

    async def list_recoverable(
        self,
        *,
        limit: int,
        max_attempts: int,
        stale_before: datetime | None = None,
    ) -> list[SagaState]:
        """Return unfinished sagas, least recently updated first."""
        statement = (
            select(SagaStateModel)
            .where(
                SagaStateModel.status.in_(("running", "compensating")),
                SagaStateModel.recovery_attempts < max_attempts,
            )
            .order_by(SagaStateModel.updated_at, SagaStateModel.id)
            .limit(limit)
        )
        if stale_before is not None:
            statement = statement.where(SagaStateModel.updated_at < stale_before)
        models = (await self._session.execute(statement)).scalars()
        return [saga_state_to_domain(model) for model in models]


class SqlAlchemyMissedCareWindowRepository:
    """The ``MissedCareWindowRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, plant_id: PlantId) -> MissedCareWindow | None:
        """Return the plant's window, or ``None``."""
        model = await self._session.get(MissedCareWindowModel, plant_id.value)
        return None if model is None else missed_care_window_to_domain(model)

    async def save(self, window: MissedCareWindow) -> None:
        """Stage the window in the caller's transaction."""
        await self._session.merge(missed_care_window_to_model(window))

    async def list_overdue(self, until: datetime, *, limit: int) -> list[MissedCareWindow]:
        """Return pending windows whose grace period expired, oldest first."""
        statement = (
            select(MissedCareWindowModel)
            .where(
                MissedCareWindowModel.state == MissedCareState.PENDING.value,
                MissedCareWindowModel.grace_deadline <= until,
            )
            .order_by(MissedCareWindowModel.grace_deadline, MissedCareWindowModel.plant_id)
            .limit(limit)
        )
        models = (await self._session.execute(statement)).scalars()
        return [missed_care_window_to_domain(model) for model in models]
