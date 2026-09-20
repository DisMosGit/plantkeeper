"""Care bounded context: watering schedules for the garden's plants."""

from __future__ import annotations

from plantkeeper.domain.care.errors import (
    CareError,
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
from plantkeeper.domain.care.schedule import CareSchedule

__all__ = [
    "CareError",
    "CareMissed",
    "CareSchedule",
    "CareScheduleCreated",
    "CareScheduleInvariantError",
    "CareScheduleVersionConflictError",
    "CareSkipped",
    "WateringCompleted",
    "WateringDue",
    "WateringNotDueError",
    "WateringRescheduled",
]
