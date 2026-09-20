"""System events published while an orchestration saga runs.

These are not bounded-context state changes: they report the progress of the
process managers in ``plantkeeper.application.sagas``, so an operator can follow a
saga through the event stream instead of only through ``write_shared.saga_state``.
They live in the domain layer because every event that travels on Kafka is a
:class:`~plantkeeper.domain.base.DomainEvent` and is catalogued here, not because
they are aggregate facts.
"""

from __future__ import annotations

from uuid import UUID

from plantkeeper.domain.base import DomainEvent


class SagaStarted(DomainEvent):
    """An orchestration saga began executing its first step."""

    saga_id: UUID
    saga_name: str


class SagaCompleted(DomainEvent):
    """Every step of an orchestration saga finished successfully."""

    saga_id: UUID
    saga_name: str


class SagaFailed(DomainEvent):
    """A step failed; the saga is compensating or has finished compensating."""

    saga_id: UUID
    saga_name: str
    error: str


class SagaCompensated(DomainEvent):
    """At least one completed step was rolled back after a failure."""

    saga_id: UUID
    saga_name: str
