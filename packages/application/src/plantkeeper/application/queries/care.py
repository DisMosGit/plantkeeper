"""Care queries."""

from __future__ import annotations

from datetime import time

from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.read_models import CareScheduleRow, ReadModelReader
from plantkeeper.application.queries.base import Query, QueryHandler
from plantkeeper.application.views import CareScheduleView, CollectionView
from plantkeeper.domain.identifiers import HouseholdId


class GetTodayCareQuery(Query):
    """Read what needs watering in a household today."""

    household_id: HouseholdId


class GetTodayCareQueryHandler(QueryHandler[GetTodayCareQuery, CollectionView[CareScheduleView]]):
    """Answer with the household's schedules that come due before midnight UTC.

    "Today" is the UTC day the request falls in. A household-local day would need
    a time zone on the household, which the domain does not model (``AGENTS.md``
    keeps this project single-tenant and unauthenticated); the boundary is stated
    rather than guessed.

    The schedule list is a question about what the system looks like, so it is
    answered from the read model: it reflects projected state, and watering a plant
    moments ago may not yet have moved its schedule out of today's list
    (``docs/cqrs.md``).
    """

    def __init__(self, read_models: ReadModelReader, clock: Clock) -> None:
        self._read_models = read_models
        self._clock = clock

    async def handle(self, query: GetTodayCareQuery) -> CollectionView[CareScheduleView]:
        """Return the due schedules, soonest first."""
        now = self._clock.now()
        end_of_day = now.replace(hour=time.max.hour, minute=59, second=59, microsecond=999999)
        rows = await self._read_models.list_due_care(query.household_id, end_of_day)
        return CollectionView[CareScheduleView](items=[_care_schedule_view(row) for row in rows])


def _care_schedule_view(row: CareScheduleRow) -> CareScheduleView:
    """Turn a read-model row into the view the API answers with."""
    return CareScheduleView(
        plant_id=row.plant_id,
        watering_interval=row.watering_interval,
        next_watering_at=row.next_watering_at,
        version=row.version,
    )
