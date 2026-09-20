"""The unit-of-work port.

A use case runs inside exactly one transaction: the aggregate rows, the outbox
rows and the idempotency row either all become visible or none of them do
(``docs/architecture.md``, "Write path").

Handlers open the unit of work, do their work and call :meth:`UnitOfWork.commit`
themselves. Nothing commits behind their back, which is what makes the
transaction boundary visible in the handler's own code.
"""

from __future__ import annotations

from types import TracebackType
from typing import Protocol, Self, runtime_checkable

from plantkeeper.application.ports.event_store import (
    EventStoreRepository,
    JournalSnapshotRepository,
)
from plantkeeper.application.ports.idempotency import IdempotencyRepository
from plantkeeper.application.ports.outbox import OutboxRepository
from plantkeeper.application.ports.repositories import (
    CareScheduleRepository,
    HouseholdRepository,
    JournalEntryRepository,
    NotificationRepository,
    PlantRepository,
    SensorRepository,
    SpeciesRepository,
    TelemetryRepository,
)
from plantkeeper.application.ports.sagas import (
    MissedCareWindowRepository,
    ProcessedEventRepository,
    SagaStateRepository,
)


@runtime_checkable
class UnitOfWork(Protocol):
    """One transaction, and the repositories that share it."""

    @property
    def plants(self) -> PlantRepository:
        """Plants, bound to this transaction's session."""
        ...

    @property
    def households(self) -> HouseholdRepository:
        """Households, bound to this transaction's session."""
        ...

    @property
    def care_schedules(self) -> CareScheduleRepository:
        """Care schedules, bound to this transaction's session."""
        ...

    @property
    def species(self) -> SpeciesRepository:
        """Catalogue species, bound to this transaction's session."""
        ...

    @property
    def sensors(self) -> SensorRepository:
        """Sensors, bound to this transaction's session."""
        ...

    @property
    def telemetry(self) -> TelemetryRepository:
        """Sensor readings, bound to this transaction's session.

        The telemetry ingress writes readings here and appends the
        ``TelemetryReceived`` they produce to the outbox in the same transaction,
        so a stored reading and the event announcing it appear together or not at
        all.
        """
        ...

    @property
    def journal_entries(self) -> JournalEntryRepository:
        """Journal entries, bound to this transaction's session."""
        ...

    @property
    def event_store(self) -> EventStoreRepository:
        """The Journal's event stream, bound to this transaction's session.

        The append and the outbox row that announces it travel in this one
        transaction, so a stored fact and its publication appear together or not at
        all (``docs/event-sourcing.md``).
        """
        ...

    @property
    def journal_snapshots(self) -> JournalSnapshotRepository:
        """Journal checkpoints, bound to this transaction's session."""
        ...

    @property
    def notifications(self) -> NotificationRepository:
        """Notifications, bound to this transaction's session."""
        ...

    @property
    def outbox(self) -> OutboxRepository:
        """The outbox, bound to this transaction's session.

        Use it to append an event that no aggregate raises, such as
        ``SpeciesSyncRequested``: such an event is a trigger, not a state
        change, so there is no aggregate to drain it from.
        """
        ...

    @property
    def idempotency(self) -> IdempotencyRepository:
        """Stored responses, bound to this transaction's session."""
        ...

    @property
    def processed_events(self) -> ProcessedEventRepository:
        """The consumer ledger, bound to this transaction's session.

        A Kafka consumer claims ``(consumer_group, event_id)`` here and commits
        it together with the effect of the delivery, which is what makes a
        redelivery harmless.
        """
        ...

    @property
    def saga_states(self) -> SagaStateRepository:
        """Saga executions, bound to this transaction's session (read side)."""
        ...

    @property
    def missed_care_windows(self) -> MissedCareWindowRepository:
        """Missed-care grace windows, bound to this transaction's session."""
        ...

    async def __aenter__(self) -> Self:
        """Begin the transaction."""
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Roll the transaction back unless :meth:`commit` already ran."""
        ...

    async def commit(self) -> None:
        """Drain the aggregates' events into the outbox and commit.

        Draining happens here, not in the repositories, so a command that
        changes three aggregates still appends their events in the order they
        happened and in the same transaction as the changes themselves.
        """
        ...

    async def rollback(self) -> None:
        """Discard the transaction."""
        ...
