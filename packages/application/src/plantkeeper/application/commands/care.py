"""Care commands: creating a schedule, completing a watering and skipping one."""

from __future__ import annotations

from datetime import timedelta

from plantkeeper.application.commands.base import Command, CommandHandler, IdempotentCommand
from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.idempotency import CREATED, commit_create
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.views import CareScheduleView
from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.identifiers import PlantId
from plantkeeper.domain.values import WateringInterval


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


class CreateCareScheduleCommand(IdempotentCommand):
    """Create a plant's first care schedule.

    Recorded by ``OnboardPlantSaga`` as a cross-context command rather than written
    by the saga itself: ``write_care`` belongs to the care context, and a process
    manager that wrote it directly would be the second writer this platform's
    rules forbid (``docs/adr/0012-saga-command-dispatch.md``).

    The cadence travels, not the due instant: the schedule's first watering is one
    interval after the command *executes*, and computing it when the row is
    recorded would make an intent dispatched a second later fail the invariant that
    the next watering cannot be in the past.
    """

    plant_id: PlantId
    watering_interval_seconds: float


class CreateCareScheduleHandler(CommandHandler[CreateCareScheduleCommand, CareScheduleView]):
    """Create the schedule and record ``CareScheduleCreated`` in one transaction."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: CreateCareScheduleCommand) -> CareScheduleView:
        """Create the schedule once, however often the command is delivered."""
        return await commit_create(
            uow=self._uow,
            clock=self._clock,
            command=command,
            view_type=CareScheduleView,
            status_code=CREATED,
            operation=lambda: self._create(command),
        )

    async def _create(self, command: CreateCareScheduleCommand) -> CareScheduleView:
        now = self._clock.now()
        interval = timedelta(seconds=command.watering_interval_seconds)
        schedule = CareSchedule.create(
            plant_id=command.plant_id,
            watering_interval=WateringInterval(value=interval),
            starts_at=now + interval,
            now=now,
        )
        await self._uow.care_schedules.add(schedule)
        return CareScheduleView.from_domain(schedule)


class DeleteCareScheduleCommand(IdempotentCommand):
    """Remove a plant's care schedule.

    The undo half of :class:`CreateCareScheduleCommand`: when the onboarding saga
    compensates a step whose create command already ran, the rollback is itself a
    recorded command, so the care context stays the only writer of ``write_care``.
    Deleting a schedule that is not there is not an error, which is what makes the
    compensation safe to deliver twice.
    """

    plant_id: PlantId


class DeleteCareScheduleHandler(CommandHandler[DeleteCareScheduleCommand, CareScheduleView | None]):
    """Delete the schedule, answering ``None`` when there was nothing to delete."""

    def __init__(self, unit_of_work: UnitOfWork) -> None:
        self._uow = unit_of_work

    async def handle(self, command: DeleteCareScheduleCommand) -> CareScheduleView | None:
        """Delete the schedule if it exists, in one transaction."""
        async with self._uow:
            schedule = await self._uow.care_schedules.get_for_update(command.plant_id)
            if schedule is None:
                return None
            await self._uow.care_schedules.delete(command.plant_id)
            await self._uow.commit()
        return CareScheduleView.from_domain(schedule)
