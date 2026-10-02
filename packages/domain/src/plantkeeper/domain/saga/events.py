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


class SagaRetrying(DomainEvent):
    """A recorded failure is being retried under the process's retry budget.

    Distinct from ``SagaStarted``: the process has run before and has a step
    history, so a reader of the stream can tell a first attempt from a retry
    without comparing the recorded state.
    """

    saga_id: UUID
    saga_name: str
    attempt: int
    error: str


class SagaParked(DomainEvent):
    """A process ran out of retries and is waiting for an operator.

    Terminal until someone resets it. Published because the alternative — a
    process that quietly stopped being retried — is indistinguishable from one
    that is still trying, and "is anything stuck?" should be answerable from the
    event stream.
    """

    saga_id: UUID
    saga_name: str
    attempts: int
    error: str
