"""The notification cursor and the stream's reader, against a real Postgres.

Resuming a stream from a cursor rests on two things a fake cannot show: Postgres
compares notification identifiers in creation order, because they are UUIDv7, and
the reader releases the session it read with — which is what lets a stream idle
without holding a database session.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from plantkeeper.domain.identifiers import HouseholdId, NotificationId, PlantId
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.infrastructure.notifications.reader import SqlAlchemyPendingNotificationReader
from plantkeeper.infrastructure.persistence.repositories.notifications import (
    SqlAlchemyNotificationRepository,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker

pytestmark = pytest.mark.integration

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def a_notification(household_id: HouseholdId) -> Notification:
    """One pending notification for the household."""
    return Notification.create(
        household_id=household_id,
        notification_type=NotificationType.WATERING_DUE,
        payload={"plant_id": str(PlantId.new())},
        now=NOW,
    )


def checked_out_connections(engine: AsyncEngine) -> int:
    """How many connections this engine currently has checked out.

    ``AsyncEngine.pool`` is typed as the abstract ``Pool``, but ``checkedout`` is
    defined on the queue pool a real engine uses — the narrower type cannot be
    named through the public interface, hence the one ignore.
    """
    return int(engine.pool.checkedout())  # type: ignore[attr-defined]


async def seed(
    session_factory: async_sessionmaker[AsyncSession], household_id: HouseholdId, count: int
) -> list[Notification]:
    """Store ``count`` notifications, oldest first, and return them."""
    notifications = [a_notification(household_id) for _ in range(count)]
    async with session_factory() as session:
        repository = SqlAlchemyNotificationRepository(session, AggregateTracker())
        for notification in notifications:
            await repository.add(notification)
        await session.commit()
    return notifications


async def test_the_cursor_returns_only_what_comes_after_it(database: str) -> None:
    household_id = HouseholdId.new()
    engine = create_async_engine(database)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        first, second, third = await seed(factory, household_id, 3)

        async with factory() as session:
            repository = SqlAlchemyNotificationRepository(session, AggregateTracker())
            assert await repository.list_pending(household_id) == [first, second, third]
            assert await repository.list_pending(household_id, since=first.id) == [second, third]
            assert await repository.list_pending(household_id, since=second.id) == [third]
            assert await repository.list_pending(household_id, since=third.id) == []
    finally:
        await engine.dispose()


async def test_a_cursor_does_not_resurrect_an_acknowledged_notification(database: str) -> None:
    household_id = HouseholdId.new()
    engine = create_async_engine(database)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        first, second, third = await seed(factory, household_id, 3)

        async with factory() as session:
            repository = SqlAlchemyNotificationRepository(session, AggregateTracker())
            stored = await repository.get(second.id)
            assert stored is not None
            stored.mark_read(now=NOW)
            await repository.save(stored)
            await session.commit()

        async with factory() as session:
            repository = SqlAlchemyNotificationRepository(session, AggregateTracker())
            assert await repository.list_pending(household_id, since=first.id) == [third]
    finally:
        await engine.dispose()


async def test_the_stream_s_reader_releases_its_session_after_every_read(database: str) -> None:
    """The requirement the stream's fan-out rests on: read, then hold nothing."""
    household_id = HouseholdId.new()
    engine = create_async_engine(database)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        first, second = await seed(factory, household_id, 2)
        reader = SqlAlchemyPendingNotificationReader(factory)

        assert [
            view.notification_id for view in await reader.pending_since(household_id, since=None)
        ] == [first.id, second.id]
        assert checked_out_connections(engine) == 0, "a read must not keep its session"

        assert [
            view.notification_id
            for view in await reader.pending_since(household_id, since=first.id)
        ] == [second.id]
        assert checked_out_connections(engine) == 0

        assert await reader.pending_since(HouseholdId.new(), since=None) == []
        assert checked_out_connections(engine) == 0
    finally:
        await engine.dispose()


async def test_a_reader_for_an_unknown_household_reads_nothing(database: str) -> None:
    household_id = HouseholdId.new()
    engine = create_async_engine(database)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        await seed(factory, household_id, 1)
        reader = SqlAlchemyPendingNotificationReader(factory)

        assert await reader.pending_since(HouseholdId.new(), since=None) == []
        assert await reader.pending_since(household_id, since=NotificationId.new()) == []
    finally:
        await engine.dispose()
