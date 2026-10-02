"""The read behind a notification stream, on a session of its own.

A stream is the one delivery form that outlives its read: it can idle for minutes
between nudges, and it must not do so holding a database session. This adapter is
what makes that true — every call opens a session, runs the *same* query handler
the request-and-wait endpoint runs, and closes the session again — so the API's
connection pool holds nothing while a stream waits, and the two delivery forms
cannot drift into two definitions of "pending".

It reads the write tables rather than a read model: delivery is a command-support
read (``docs/cqrs.md``), and a notification is answered from the store that owns
it.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.application.queries.notifications import (
    ListPendingNotificationsHandler,
    ListPendingNotificationsQuery,
)
from plantkeeper.application.views import NotificationView
from plantkeeper.domain.identifiers import HouseholdId, NotificationId
from plantkeeper.infrastructure.persistence.repositories.notifications import (
    SqlAlchemyNotificationRepository,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker


class SqlAlchemyPendingNotificationReader:
    """The ``PendingNotificationReader`` port over the session factory."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        """Hold the process's factory; each read opens and releases its own session."""
        self._session_factory = session_factory

    async def pending_since(
        self, household_id: HouseholdId, *, since: NotificationId | None
    ) -> list[NotificationView]:
        """Answer with the household's pending notifications after ``since``."""
        async with self._session_factory() as session:
            handler = ListPendingNotificationsHandler(
                SqlAlchemyNotificationRepository(session, AggregateTracker())
            )
            pending = await handler.handle(
                ListPendingNotificationsQuery(household_id=household_id, since=since)
            )
            return pending.items
