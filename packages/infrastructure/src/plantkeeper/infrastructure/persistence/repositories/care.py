"""SQLAlchemy repository of the Care context."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.application.ports.sagas import MissedCareState
from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.identifiers import HouseholdId, PlantId
from plantkeeper.infrastructure.persistence.mappers.care import (
    care_schedule_to_domain,
    care_schedule_to_model,
)
from plantkeeper.infrastructure.persistence.models.care import (
    CareScheduleModel,
    MissedCareWindowModel,
)
from plantkeeper.infrastructure.persistence.models.garden import PlantModel
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker


class SqlAlchemyCareScheduleRepository:
    """The ``CareScheduleRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession, tracker: AggregateTracker) -> None:
        self._session = session
        self._tracker = tracker

    async def add(self, schedule: CareSchedule) -> None:
        """Insert a new schedule."""
        self._session.add(care_schedule_to_model(schedule))
        self._tracker.track(schedule)

    async def get(self, plant_id: PlantId) -> CareSchedule | None:
        """Return the schedule, or ``None``. Takes no lock."""
        model = await self._session.get(CareScheduleModel, plant_id.value)
        return None if model is None else care_schedule_to_domain(model)

    async def get_for_update(self, plant_id: PlantId) -> CareSchedule | None:
        """Return the schedule, locking it for this transaction."""
        statement = (
            select(CareScheduleModel)
            .where(CareScheduleModel.plant_id == plant_id.value)
            .with_for_update()
        )
        model = (await self._session.execute(statement)).scalar_one_or_none()
        return None if model is None else care_schedule_to_domain(model)

    async def save(self, schedule: CareSchedule) -> None:
        """Persist the schedule's current state and version."""
        await self._session.merge(care_schedule_to_model(schedule))
        self._tracker.track(schedule)

    async def delete(self, plant_id: PlantId) -> None:
        """Remove the schedule, if it exists."""
        model = await self._session.get(CareScheduleModel, plant_id.value)
        if model is not None:
            await self._session.delete(model)

    async def list_due(self, household_id: HouseholdId, until: datetime) -> list[CareSchedule]:
        """List a household's schedules that come due at or before ``until``.

        This joins the Garden context's plants table to exclude removed plants.
        Both schemas live in one database, and the join is read-only: no context
        gains a dependency on the other's code, which is what the independence
        rule actually protects.
        """
        statement = (
            select(CareScheduleModel)
            .join(PlantModel, PlantModel.id == CareScheduleModel.plant_id)
            .where(
                PlantModel.household_id == household_id.value,
                PlantModel.removed.is_(False),
                CareScheduleModel.next_watering_at <= until,
            )
            .order_by(CareScheduleModel.next_watering_at, CareScheduleModel.plant_id)
        )
        models = (await self._session.execute(statement)).scalars()
        return [care_schedule_to_domain(model) for model in models]

    async def list_due_without_pending_window(
        self, until: datetime, *, limit: int
    ) -> list[CareSchedule]:
        """List due schedules that are not already inside a grace window.

        The anti-join is what keeps the missed-care scheduler from recording
        ``WateringDue`` again on every tick: once a window is open, the schedule
        stops being "newly due" until the window closes.
        """
        pending_window = (
            select(MissedCareWindowModel.plant_id)
            .where(
                MissedCareWindowModel.plant_id == CareScheduleModel.plant_id,
                MissedCareWindowModel.state == MissedCareState.PENDING.value,
            )
            .exists()
        )
        statement = (
            select(CareScheduleModel)
            .join(PlantModel, PlantModel.id == CareScheduleModel.plant_id)
            .where(
                PlantModel.removed.is_(False),
                CareScheduleModel.next_watering_at <= until,
                ~pending_window,
            )
            .order_by(CareScheduleModel.next_watering_at, CareScheduleModel.plant_id)
            .limit(limit)
        )
        models = (await self._session.execute(statement)).scalars()
        return [care_schedule_to_domain(model) for model in models]
