"""The CareSchedule aggregate: when one plant should be watered next."""

from __future__ import annotations

from datetime import datetime

from plantkeeper.domain.base import AggregateRoot
from plantkeeper.domain.care.errors import (
    CareScheduleInvariantError,
    CareScheduleVersionConflictError,
    WateringNotDueError,
)
from plantkeeper.domain.care.events import (
    CareMissed,
    CareScheduleCreated,
    CareSkipped,
    WateringCompleted,
    WateringDue,
    WateringRescheduled,
)
from plantkeeper.domain.identifiers import PlantId
from plantkeeper.domain.values import WateringInterval


class CareSchedule(AggregateRoot[PlantId]):
    """The watering schedule of one plant.

    A schedule is identified by its plant: a plant has exactly one schedule, so
    :class:`PlantId` doubles as the aggregate identifier. Every change goes
    through ``expected_version`` (optimistic locking): a caller that read version
    N can only write version N, which turns a lost update into a
    :class:`CareScheduleVersionConflictError` instead of silent data loss.
    """

    def __init__(
        self,
        plant_id: PlantId,
        *,
        watering_interval: WateringInterval,
        next_watering_at: datetime,
        version: int = 1,
    ) -> None:
        """Rebuild a schedule from its stored state (no events are recorded)."""
        super().__init__(plant_id)
        self._watering_interval = watering_interval
        self._next_watering_at = next_watering_at
        self._version = version

    @classmethod
    def create(
        cls,
        *,
        plant_id: PlantId,
        watering_interval: WateringInterval,
        starts_at: datetime,
        now: datetime,
    ) -> CareSchedule:
        """Create a schedule whose first watering is due at ``starts_at``.

        Raises :class:`CareScheduleInvariantError` when ``starts_at`` is in the
        past relative to ``now``.
        """
        if starts_at < now:
            raise CareScheduleInvariantError(
                f"next watering {starts_at.isoformat()} is before now {now.isoformat()}"
            )
        schedule = cls(plant_id, watering_interval=watering_interval, next_watering_at=starts_at)
        schedule._record(
            CareScheduleCreated(
                plant_id=plant_id,
                watering_interval=watering_interval,
                next_watering_at=starts_at,
                occurred_at=now,
            )
        )
        return schedule

    @property
    def plant_id(self) -> PlantId:
        """The plant this schedule belongs to."""
        return self.id

    @property
    def watering_interval(self) -> WateringInterval:
        """How often the plant should be watered."""
        return self._watering_interval

    @property
    def next_watering_at(self) -> datetime:
        """When the plant is next due for watering."""
        return self._next_watering_at

    @property
    def version(self) -> int:
        """The optimistic-locking version, incremented by every change."""
        return self._version

    def mark_due(self, *, now: datetime) -> None:
        """Record :class:`WateringDue` once the schedule has come due.

        This reports a fact rather than changing the schedule, so it neither
        requires ``expected_version`` nor bumps ``version``.
        """
        if now < self._next_watering_at:
            raise WateringNotDueError(
                f"plant {self.id} is due at {self._next_watering_at.isoformat()}, not yet"
            )
        self._record(WateringDue(plant_id=self.id, due_at=self._next_watering_at, occurred_at=now))

    def complete_watering(self, *, now: datetime, expected_version: int) -> None:
        """Record a completed watering and schedule the next one."""
        self._ensure_version(expected_version)
        self._next_watering_at = now + self._watering_interval.value
        self._version += 1
        self._record(
            WateringCompleted(
                plant_id=self.id,
                completed_at=now,
                next_watering_at=self._next_watering_at,
                occurred_at=now,
            )
        )

    def reschedule(
        self,
        *,
        next_watering_at: datetime,
        now: datetime,
        expected_version: int,
        reason: str | None = None,
    ) -> None:
        """Move the next watering moment, for example because the soil is dry.

        Raises :class:`CareScheduleInvariantError` when the new moment is in the
        past.
        """
        self._ensure_version(expected_version)
        if next_watering_at < now:
            raise CareScheduleInvariantError(
                f"next watering {next_watering_at.isoformat()} is before now {now.isoformat()}"
            )
        previous_next_watering_at = self._next_watering_at
        self._next_watering_at = next_watering_at
        self._version += 1
        self._record(
            WateringRescheduled(
                plant_id=self.id,
                previous_next_watering_at=previous_next_watering_at,
                next_watering_at=next_watering_at,
                reason=reason,
                occurred_at=now,
            )
        )

    def skip(self, *, now: datetime, expected_version: int) -> None:
        """Skip this watering and move to the following interval."""
        self._ensure_version(expected_version)
        self._next_watering_at = self._next_watering_at + self._watering_interval.value
        self._version += 1
        self._record(
            CareSkipped(
                plant_id=self.id,
                skipped_at=now,
                next_watering_at=self._next_watering_at,
                occurred_at=now,
            )
        )

    def mark_missed(self, *, now: datetime, expected_version: int) -> None:
        """Record that the grace period expired and move to the next interval."""
        self._ensure_version(expected_version)
        self._next_watering_at = self._next_watering_at + self._watering_interval.value
        self._version += 1
        self._record(
            CareMissed(plant_id=self.id, next_watering_at=self._next_watering_at, occurred_at=now)
        )

    def _ensure_version(self, expected_version: int) -> None:
        if expected_version != self._version:
            raise CareScheduleVersionConflictError(
                f"care schedule {self.id} is at version {self._version}, "
                f"the caller expected {expected_version}"
            )
