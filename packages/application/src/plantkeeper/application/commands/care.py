"""Care commands: completing and skipping a watering."""

from __future__ import annotations

from plantkeeper.application.commands.base import Command, CommandHandler
from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.views import CareScheduleView
from plantkeeper.domain.identifiers import PlantId


class WaterPlantCommand(Command):
    """Record that a plant was watered now.

    ``expected_version`` is optional and exists for clients that read the
    schedule first: supplying the version they saw turns a concurrent watering
    into an explicit conflict. Omitting it is safe as well — the schedule row is
    locked for the whole transaction, so a lost update is impossible either way.
    """

    plant_id: PlantId
    expected_version: int | None = None


class WaterPlantHandler(CommandHandler[WaterPlantCommand, CareScheduleView]):
    """Advance the schedule and the plant together, in one transaction.

    Both aggregates change: the schedule moves to its next watering, and the
    plant remembers when it was last watered so that "not twice within an hour"
    can be enforced. Two aggregates in two schemas still commit as one unit.
    """

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: WaterPlantCommand) -> CareScheduleView:
        """Complete the watering, or raise instead of half-doing it."""
        now = self._clock.now()
        async with self._uow:
            schedule = await self._uow.care_schedules.get_for_update(command.plant_id)
            if schedule is None:
                raise NotFoundError(f"plant {command.plant_id} has no care schedule")
            plant = await self._uow.plants.get(command.plant_id)
            if plant is None:
                raise NotFoundError(f"plant {command.plant_id} does not exist")

            # Raises PlantWateringTooSoonError when the last watering is under an
            # hour old, before anything is written.
            plant.water(now=now)
            schedule.complete_watering(
                now=now,
                expected_version=(
                    command.expected_version
                    if command.expected_version is not None
                    else schedule.version
                ),
            )

            await self._uow.plants.save(plant)
            await self._uow.care_schedules.save(schedule)
            await self._uow.commit()
        return CareScheduleView.from_domain(schedule)


class SkipWateringCommand(Command):
    """Skip this watering and move to the following interval."""

    plant_id: PlantId
    expected_version: int | None = None


class SkipWateringHandler(CommandHandler[SkipWateringCommand, CareScheduleView]):
    """Advance the schedule without watering the plant."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: SkipWateringCommand) -> CareScheduleView:
        """Skip the watering and publish ``CareSkipped``."""
        async with self._uow:
            schedule = await self._uow.care_schedules.get_for_update(command.plant_id)
            if schedule is None:
                raise NotFoundError(f"plant {command.plant_id} has no care schedule")
            schedule.skip(
                now=self._clock.now(),
                expected_version=(
                    command.expected_version
                    if command.expected_version is not None
                    else schedule.version
                ),
            )
            await self._uow.care_schedules.save(schedule)
            await self._uow.commit()
        return CareScheduleView.from_domain(schedule)
