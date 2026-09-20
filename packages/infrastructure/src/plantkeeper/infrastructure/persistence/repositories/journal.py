"""SQLAlchemy repository of the Journal context."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.infrastructure.persistence.mappers.journal import (
    journal_entry_to_domain,
    journal_entry_to_model,
)
from plantkeeper.infrastructure.persistence.models.journal import JournalEntryModel
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker


class SqlAlchemyJournalEntryRepository:
    """The ``JournalEntryRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession, tracker: AggregateTracker) -> None:
        self._session = session
        self._tracker = tracker

    async def add(self, entry: JournalEntry) -> None:
        """Append an entry."""
        self._session.add(journal_entry_to_model(entry))
        self._tracker.track(entry)

    async def get(self, entry_id: JournalEntryId) -> JournalEntry | None:
        """Return the entry, or ``None``."""
        model = await self._session.get(JournalEntryModel, entry_id.value)
        return None if model is None else journal_entry_to_domain(model)

    async def save(self, entry: JournalEntry) -> None:
        """Persist the entry.

        The aggregate is immutable, so this exists only to satisfy the generic
        repository contract; it is identical to :meth:`add` for a stored entry.
        """
        await self._session.merge(journal_entry_to_model(entry))
        self._tracker.track(entry)

    async def delete(self, entry_id: JournalEntryId) -> None:
        """Remove the entry, if it exists."""
        model = await self._session.get(JournalEntryModel, entry_id.value)
        if model is not None:
            await self._session.delete(model)

    async def list_by_plant(self, plant_id: PlantId) -> list[JournalEntry]:
        """List a plant's entries in chronological order.

        Ordering matches ``JournalStream.entries`` (``occurred_at``, then id) so
        the stream rebuilt from the database is the stream the domain expects.
        """
        statement = (
            select(JournalEntryModel)
            .where(JournalEntryModel.plant_id == plant_id.value)
            .order_by(JournalEntryModel.occurred_at, JournalEntryModel.id)
        )
        models = (await self._session.execute(statement)).scalars()
        return [journal_entry_to_domain(model) for model in models]
