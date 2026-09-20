"""SQLAlchemy repositories of the Journal's event store.

The store's optimistic lock is the database's own ``(stream_id, version)`` unique
key, not a ``SELECT ... FOR UPDATE``: an append flushes immediately, so a writer
that read version N learns it lost the race at the append rather than at the commit,
and the unit of work's rollback then leaves nothing behind.
"""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.application.errors import EventStoreConcurrencyError
from plantkeeper.application.ports.event_store import StoredEvent
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.identifiers import PlantId
from plantkeeper.domain.journal.state import JournalState
from plantkeeper.infrastructure.persistence.mappers.journal import (
    event_store_model_from_event,
    snapshot_model_from_state,
    state_from_snapshot_model,
    stored_event_from_model,
)
from plantkeeper.infrastructure.persistence.models.journal import (
    EventStoreModel,
    JournalSnapshotModel,
)


class SqlAlchemyEventStoreRepository:
    """The ``EventStoreRepository`` port over ``write_journal.event_store``."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(
        self,
        stream_id: PlantId,
        *,
        expected_version: int,
        events: Sequence[DomainEvent],
    ) -> None:
        """Append ``events`` at consecutive versions after ``expected_version``."""
        models = [
            event_store_model_from_event(
                event, stream_id=stream_id, version=expected_version + offset
            )
            for offset, event in enumerate(events, start=1)
        ]
        self._session.add_all(models)
        try:
            await self._session.flush()
        except IntegrityError as exc:
            raise EventStoreConcurrencyError(
                f"stream {stream_id} already holds a version between "
                f"{expected_version + 1} and {expected_version + len(models)}"
            ) from exc

    async def load_stream(self, stream_id: PlantId, *, after_version: int = 0) -> list[StoredEvent]:
        """Read the stream's events strictly after ``after_version``, oldest first."""
        statement = (
            select(EventStoreModel)
            .where(
                EventStoreModel.stream_id == stream_id.value,
                EventStoreModel.version > after_version,
            )
            .order_by(EventStoreModel.version)
        )
        models = (await self._session.execute(statement)).scalars()
        return [stored_event_from_model(model) for model in models]

    async def load_all(self, *, after_position: int = 0, limit: int = 1000) -> list[StoredEvent]:
        """Read events across every stream in global append order."""
        statement = (
            select(EventStoreModel)
            .where(EventStoreModel.global_position > after_position)
            .order_by(EventStoreModel.global_position)
            .limit(limit)
        )
        models = (await self._session.execute(statement)).scalars()
        return [stored_event_from_model(model) for model in models]


class SqlAlchemyJournalSnapshotRepository:
    """The ``JournalSnapshotRepository`` port over ``write_journal.journal_snapshots``."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def latest(self, plant_id: PlantId) -> JournalState | None:
        """Return the newest checkpoint, which the primary key's index serves."""
        statement = (
            select(JournalSnapshotModel)
            .where(JournalSnapshotModel.stream_id == plant_id.value)
            .order_by(JournalSnapshotModel.version.desc())
            .limit(1)
        )
        model = (await self._session.execute(statement)).scalars().first()
        return None if model is None else state_from_snapshot_model(model)

    async def save(self, state: JournalState) -> None:
        """Stage the checkpoint in the caller's transaction."""
        self._session.add(snapshot_model_from_state(state))
