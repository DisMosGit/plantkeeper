"""The unit of work over one SQLAlchemy session.

Everything a write use case does shares this session, so a single
``session.commit()`` makes the aggregate rows, the outbox rows and the
idempotency row visible at the same instant — or none of them
(``docs/adr/0003-write-side-outbox.md``).

The session itself is provided per request (see the Dishka providers), not
created here: a query and a command in the same request then reuse one
connection, and the request's scope owns the closing.
"""

from __future__ import annotations

from types import TracebackType
from typing import Self

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.application.errors import ConcurrentWriteError
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
from plantkeeper.infrastructure.persistence.repositories.care import (
    SqlAlchemyCareScheduleRepository,
)
from plantkeeper.infrastructure.persistence.repositories.catalog import (
    SqlAlchemySpeciesRepository,
)
from plantkeeper.infrastructure.persistence.repositories.garden import (
    SqlAlchemyHouseholdRepository,
    SqlAlchemyPlantRepository,
)
from plantkeeper.infrastructure.persistence.repositories.idempotency import (
    SqlAlchemyIdempotencyRepository,
)
from plantkeeper.infrastructure.persistence.repositories.journal import (
    SqlAlchemyJournalEntryRepository,
)
from plantkeeper.infrastructure.persistence.repositories.notifications import (
    SqlAlchemyNotificationRepository,
)
from plantkeeper.infrastructure.persistence.repositories.outbox import (
    SqlAlchemyOutboxRepository,
)
from plantkeeper.infrastructure.persistence.repositories.sagas import (
    SqlAlchemyMissedCareWindowRepository,
    SqlAlchemyProcessedEventRepository,
    SqlAlchemySagaStateRepository,
)
from plantkeeper.infrastructure.persistence.repositories.telemetry import (
    SqlAlchemySensorRepository,
    SqlAlchemyTelemetryRepository,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker


class SqlAlchemyUnitOfWork:
    """The ``UnitOfWork`` port over one :class:`~sqlalchemy.ext.asyncio.AsyncSession`.

    The property annotations name the *ports*, not the implementations, so
    ``mypy`` checks that each repository really satisfies its protocol.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._tracker = AggregateTracker()
        self._committed = False

        self._plants = SqlAlchemyPlantRepository(session, self._tracker)
        self._households = SqlAlchemyHouseholdRepository(session, self._tracker)
        self._care_schedules = SqlAlchemyCareScheduleRepository(session, self._tracker)
        self._species = SqlAlchemySpeciesRepository(session, self._tracker)
        self._sensors = SqlAlchemySensorRepository(session, self._tracker)
        self._telemetry = SqlAlchemyTelemetryRepository(session)
        self._journal_entries = SqlAlchemyJournalEntryRepository(session, self._tracker)
        self._notifications = SqlAlchemyNotificationRepository(session, self._tracker)
        self._outbox = SqlAlchemyOutboxRepository(session)
        self._idempotency = SqlAlchemyIdempotencyRepository(session)
        self._processed_events = SqlAlchemyProcessedEventRepository(session)
        self._saga_states = SqlAlchemySagaStateRepository(session)
        self._missed_care_windows = SqlAlchemyMissedCareWindowRepository(session)

    @property
    def plants(self) -> PlantRepository:
        """Plants, bound to this transaction."""
        return self._plants

    @property
    def households(self) -> HouseholdRepository:
        """Households, bound to this transaction."""
        return self._households

    @property
    def care_schedules(self) -> CareScheduleRepository:
        """Care schedules, bound to this transaction."""
        return self._care_schedules

    @property
    def species(self) -> SpeciesRepository:
        """Catalogue species, bound to this transaction."""
        return self._species

    @property
    def sensors(self) -> SensorRepository:
        """Sensors, bound to this transaction."""
        return self._sensors

    @property
    def telemetry(self) -> TelemetryRepository:
        """Sensor readings, bound to this transaction."""
        return self._telemetry

    @property
    def journal_entries(self) -> JournalEntryRepository:
        """Journal entries, bound to this transaction."""
        return self._journal_entries

    @property
    def notifications(self) -> NotificationRepository:
        """Notifications, bound to this transaction."""
        return self._notifications

    @property
    def outbox(self) -> OutboxRepository:
        """The outbox, bound to this transaction."""
        return self._outbox

    @property
    def idempotency(self) -> IdempotencyRepository:
        """Stored responses, bound to this transaction."""
        return self._idempotency

    @property
    def processed_events(self) -> ProcessedEventRepository:
        """The consumer ledger, bound to this transaction."""
        return self._processed_events

    @property
    def saga_states(self) -> SagaStateRepository:
        """Saga executions, bound to this transaction."""
        return self._saga_states

    @property
    def missed_care_windows(self) -> MissedCareWindowRepository:
        """Missed-care grace windows, bound to this transaction."""
        return self._missed_care_windows

    async def __aenter__(self) -> Self:
        """Enter the transaction.

        Nothing is begun explicitly: SQLAlchemy opens a transaction on first use,
        which keeps a handler that only reads from taking a write lock.
        """
        self._committed = False
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Roll back unless the body committed."""
        if exc_type is not None or not self._committed:
            await self.rollback()

    async def commit(self) -> None:
        """Append every recorded event to the outbox, then commit.

        The events are drained from the aggregates *before* the commit, so they
        travel in the same transaction as the state change that produced them.
        A failure anywhere in between aborts the request and the aggregates go
        with it, so nothing is lost by draining eagerly.

        A unique-constraint violation (two requests racing on the same natural
        key) is translated into an application error: the caller can then decide
        what the race means instead of handling a driver exception.
        """
        for event in self._tracker.collect_events():
            await self._outbox.append(event)
        try:
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            raise ConcurrentWriteError(f"a unique constraint was violated: {exc}") from exc
        self._committed = True

    async def rollback(self) -> None:
        """Discard the transaction."""
        self._committed = False
        await self._session.rollback()
