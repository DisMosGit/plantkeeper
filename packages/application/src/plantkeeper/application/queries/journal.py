"""Journal queries: the whole timeline, and the journal as of a date.

Both queries ask the event store, not the tabular mirror: the store is the journal's
source of truth, and replaying it is what makes "state on any date" a property of the
data rather than of a table someone remembered to keep updated.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time

from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.journal.store import load_journal_aggregate
from plantkeeper.application.ports.event_store import (
    EventStoreRepository,
    JournalSnapshotRepository,
)
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
    """
    if await plants.get(plant_id) is None:
        raise NotFoundError(f"plant {plant_id} does not exist")


class GetJournalTimelineQuery(Query):
    """Read one plant's whole journal, oldest first."""

    plant_id: PlantId


class GetJournalTimelineHandler(
    QueryHandler[GetJournalTimelineQuery, CollectionView[JournalEntryView]]
):
    """Answer with every entry, in the order the stream replays."""

    def __init__(
        self,
        plants: PlantRepository,
        event_store: EventStoreRepository,
        snapshots: JournalSnapshotRepository,
    ) -> None:
        self._plants = plants
        self._event_store = event_store
        self._snapshots = snapshots

    async def handle(self, query: GetJournalTimelineQuery) -> CollectionView[JournalEntryView]:
        """Return the plant's entries in chronological order."""
        await require_plant(self._plants, query.plant_id)
        aggregate = await load_journal_aggregate(
            event_store=self._event_store,
            snapshots=self._snapshots,
            plant_id=query.plant_id,
        )
        return CollectionView[JournalEntryView](
            items=[JournalEntryView.from_domain(entry) for entry in aggregate.entries]
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
