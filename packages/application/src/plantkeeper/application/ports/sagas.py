"""Ports the sagas and their consumers need from the outside world.

Three concerns live here, all of them "state that survives a process":

* :class:`ProcessedEventRepository` — the consumer-side idempotency ledger. Every
  Kafka consumer claims ``(consumer_group, event_id)`` before it does any work, so
  an at-least-once redelivery becomes a no-op instead of a second effect.
* :class:`SagaStateRepository` — read access to ``write_shared.saga_state``. The
  write side of that table belongs to the saga engine's storage, and its own
  recovery query answers with identifiers only; this port is where a caller reads
  the rows themselves.
* :class:`MissedCareWindowRepository` — the timer state of the *choreography*
  MissedCareSaga. It has no central process manager to keep it, so the window
  between "watering came due" and "the grace period expired" is a row.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable
from uuid import UUID

from cqrs.saga.storage.enums import SagaStatus
from pydantic import AwareDatetime, BaseModel, ConfigDict

from plantkeeper.domain.identifiers import HouseholdId, PlantId


class SagaState(BaseModel):
    """One row of ``write_shared.saga_state``, as a reader needs it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    saga_id: UUID
    saga_name: str
    status: SagaStatus
    version: int
    recovery_attempts: int
    created_at: AwareDatetime
    updated_at: AwareDatetime


@runtime_checkable
class ProcessedEventRepository(Protocol):
    """The ``(consumer_group, event_id)`` ledger."""

    async def claim(self, consumer_group: str, event_id: UUID) -> bool:
        """Record the delivery, answering ``True`` only for the first one.

        The claim is written in the caller's transaction, so a handler failure
        releases it and the broker's redelivery starts from nothing.
        """
        ...


@runtime_checkable
class SagaStateRepository(Protocol):
    """Read access to the saga executions the engine persists."""

    async def get(self, saga_id: UUID) -> SagaState | None:
        """Return the saga's row, or ``None`` when it never existed."""
        ...

    async def list_recoverable(
        self,
        *,
        limit: int,
        max_attempts: int,
        stale_before: datetime | None = None,
    ) -> list[SagaState]:
        """Return sagas left in a non-terminal state, oldest update first.

        ``max_attempts`` excludes a saga that failed recovery repeatedly;
        ``stale_before`` excludes one that another worker is touching right now.
        """
        ...


class MissedCareState(StrEnum):
    """Where a plant is in the missed-care grace window."""

    PENDING = "pending"
    """Watering came due and the grace period is running."""

    SATISFIED = "satisfied"
    """The watering was completed in time."""

    MISSED = "missed"
    """The grace period expired; ``CareMissed`` was recorded."""


class MissedCareWindow(BaseModel):
    """The grace-period timer of one plant.

    Persisted rather than held in memory: the scheduler and the ``care.events``
    consumer run in different tasks, and the window has to outlive both.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    plant_id: PlantId
    household_id: HouseholdId
    due_at: AwareDatetime
    grace_deadline: AwareDatetime
    state: MissedCareState
    updated_at: AwareDatetime


@runtime_checkable
class MissedCareWindowRepository(Protocol):
    """Read and write access to ``write_care.missed_care_windows``."""

    async def get(self, plant_id: PlantId) -> MissedCareWindow | None:
        """Return the plant's current window, or ``None`` when it has none."""
        ...

    async def save(self, window: MissedCareWindow) -> None:
        """Stage the window in the current transaction."""
        ...

    async def list_overdue(self, until: datetime, *, limit: int) -> list[MissedCareWindow]:
        """Return pending windows whose grace period expired at or before ``until``."""
        ...
