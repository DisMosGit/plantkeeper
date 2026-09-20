"""Care invariant violations."""

from __future__ import annotations

from plantkeeper.domain.base import DomainError


class CareError(DomainError):
    """Base class for every Care rule violation."""


class CareScheduleInvariantError(CareError):
    """The watering interval or the next watering moment is not allowed."""


class CareScheduleVersionConflictError(CareError):
    """The schedule changed since the caller read it (optimistic locking)."""


class WateringNotDueError(CareError):
    """The schedule is not due yet, so ``WateringDue`` must not be raised."""
