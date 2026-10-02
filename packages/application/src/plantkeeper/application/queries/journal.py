"""Journal queries: the whole timeline, and the journal as of a date.

The two questions are answered from two different places, on purpose. Browsing the
timeline is a question about what the system looks like, so it is served from the
``journal_entries`` read model the ``JournalProjection`` maintains. "What did it look
like on that date?" is a replay, and a replay is a question only the write side's
event store can answer: the store is the journal's source of truth, and replaying it
is what makes "state on any date" a property of the data rather than of a table
someone remembered to keep updated.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time

from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.journal.store import load_journal_aggregate
from plantkeeper.application.ports.event_store import (
    EventStoreRepository,
    JournalSnapshotRepository,
)
from plantkeeper.application.ports.read_models import JournalTimelineRow, ReadModelReader
from plantkeeper.application.ports.repositories import PlantRepository
from plantkeeper.application.queries.base import Query, QueryHandler
from plantkeeper.application.views import CollectionView, JournalEntryView, JournalStateView
from plantkeeper.domain.identifiers import PlantId

DAY_END = time.max
"""What a bare calendar date means as a cut-off: the last moment of that UTC day."""


async def require_plant(plants: PlantRepository, plant_id: PlantId) -> None:
    """Fail with a 404 when the plant the caller asked about does not exist.

    A journal is per plant, so an unknown plant and an empty journal are different
    answers; saying which is which is the same choice every other read endpoint makes.

    This is the write-side check, used by the replay query: it answers from the store
    being replayed rather than from a read model that may not have caught up. The
    timeline, which is served from the read model, asks that model instead.
    """
    if await plants.get(plant_id) is None:
        raise NotFoundError(f"plant {plant_id} does not exist")


class GetJournalTimelineQuery(Query):
    """Read one plant's whole journal, oldest first."""

    plant_id: PlantId


class GetJournalTimelineHandler(
    QueryHandler[GetJournalTimelineQuery, CollectionView[JournalEntryView]]
):
    """Answer with the projected timeline, in the order the entries were recorded.

    The answer reflects projected state: an entry added moments ago may not be listed
    until ``JournalProjection`` has seen its ``JournalEntryAdded`` (``docs/cqrs.md``).
    """

    def __init__(self, read_models: ReadModelReader) -> None:
        self._read_models = read_models

    async def handle(self, query: GetJournalTimelineQuery) -> CollectionView[JournalEntryView]:
        """Return the plant's projected entries in chronological order."""
        if not await self._read_models.plant_exists(query.plant_id):
            raise NotFoundError(f"plant {query.plant_id} does not exist")
        rows = await self._read_models.list_journal_entries(query.plant_id)
        return CollectionView[JournalEntryView](items=[_journal_entry_view(row) for row in rows])


def _journal_entry_view(row: JournalTimelineRow) -> JournalEntryView:
    """Turn a read-model row into the view the API answers with."""
    return JournalEntryView(
        entry_id=row.entry_id,
        plant_id=row.plant_id,
        entry_type=row.entry_type,
        occurred_at=row.occurred_at,
        note=row.note,
    )


class GetJournalAtDateQuery(Query):
    """Read the journal for care that happened on or before ``date``."""

    plant_id: PlantId
    date: date


class GetJournalAtDateHandler(QueryHandler[GetJournalAtDateQuery, JournalStateView]):
    """Replay the stream and answer with the state as of the end of that day.

    The cut-off is the care moment and the day is inclusive, so ``2026-03-01`` means
    everything that happened up to and including that day — which is the question a
    household actually asks ("what had I done by then?").
    """

    def __init__(
        self,
        plants: PlantRepository,
        event_store: EventStoreRepository,
        snapshots: JournalSnapshotRepository,
    ) -> None:
        self._plants = plants
        self._event_store = event_store
        self._snapshots = snapshots

    async def handle(self, query: GetJournalAtDateQuery) -> JournalStateView:
        """Return the journal replayed to the end of ``query.date``."""
        await require_plant(self._plants, query.plant_id)
        as_of = datetime.combine(query.date, DAY_END, tzinfo=UTC)
        aggregate = await load_journal_aggregate(
            event_store=self._event_store,
            snapshots=self._snapshots,
            plant_id=query.plant_id,
        )
        return JournalStateView.from_state(aggregate.state_at(as_of), as_of=as_of)
