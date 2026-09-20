"""Care queries."""

from __future__ import annotations

from datetime import time

from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.repositories import CareScheduleRepository
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
    """

    def __init__(self, care_schedules: CareScheduleRepository, clock: Clock) -> None:
        self._care_schedules = care_schedules
        self._clock = clock

    async def handle(self, query: GetTodayCareQuery) -> CollectionView[CareScheduleView]:
        """Return the due schedules, soonest first."""
        now = self._clock.now()
        end_of_day = now.replace(hour=time.max.hour, minute=59, second=59, microsecond=999999)
        schedules = await self._care_schedules.list_due(query.household_id, end_of_day)
        return CollectionView[CareScheduleView](
            items=[CareScheduleView.from_domain(s) for s in schedules]
        )
