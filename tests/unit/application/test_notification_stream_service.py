"""The notification stream: cursor, fan-out and degradation, without a wire.

The service is the part of streaming that is not HTTP: it decides when to read,
what the cursor is, which stream a nudge wakes and what to do when the household
signal is gone. A fake channel and a fake reader make all four deterministic —
the real ones are exercised by ``tests/integration`` and ``tests/e2e``.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest

from plantkeeper.application.notifications.stream import (
    HouseholdSignalFanout,
    NotificationArrival,
    NotificationStreamEvent,
    NotificationStreamService,
    StreamKeepAlive,
    StreamOpened,
)
from plantkeeper.application.ports.notifications import NotificationChannelError
from plantkeeper.application.views import NotificationView
from plantkeeper.domain.identifiers import HouseholdId, NotificationId, PlantId
from plantkeeper.domain.notifications.values import NotificationType

HOUSEHOLD = HouseholdId.new()
OTHER_HOUSEHOLD = HouseholdId.new()


def a_notification() -> NotificationView:
    """One pending notification, as the reader would hand it over."""
    return NotificationView(
        notification_id=NotificationId.new(),
        household_id=HOUSEHOLD,
        notification_type=NotificationType.WATERING_DUE,
        payload={"plant_id": str(PlantId.new())},
        created_at=datetime.now(UTC),
        read_at=None,
    )


class FakeSubscription:
    """One open channel subscription the test nudges and breaks."""

    def __init__(self) -> None:
        self._nudge = asyncio.Event()
        self._failure: NotificationChannelError | None = None

    async def wait(self, timeout: float) -> bool:
        """Answer a nudge, the deadline, or the failure the test injected."""
        if self._failure is not None:
            raise self._failure
        try:
            async with asyncio.timeout(timeout):
                await self._nudge.wait()
        except TimeoutError:
            return False
        self._nudge.clear()
        if self._failure is not None:
            raise self._failure
        return True

    def nudge(self) -> None:
        """Signal the household, as a published nudge does."""
        self._nudge.set()

    def fail(self) -> None:
        """Break the channel while the subscriber is attached."""
        self._failure = NotificationChannelError("valkey went away")
        self._nudge.set()


class FakeChannel:
    """A presence channel the test drives: one subscription per household."""

    def __init__(self) -> None:
        self.subscriptions: dict[HouseholdId, list[FakeSubscription]] = defaultdict(list)
        self.opened = 0
        self.closed = 0
        self.subscribe_failure: NotificationChannelError | None = None

    @asynccontextmanager
    async def subscribe(self, household_id: HouseholdId) -> AsyncIterator[FakeSubscription]:
        """Open a subscription, or fail the way an unreachable Valkey does."""
        if self.subscribe_failure is not None:
            raise self.subscribe_failure
        subscription = FakeSubscription()
        self.subscriptions[household_id].append(subscription)
        self.opened += 1
        try:
            yield subscription
        finally:
            self.subscriptions[household_id].remove(subscription)
            self.closed += 1

    async def publish(self, household_id: HouseholdId) -> None:
        """Nudge the households's open subscriptions, and only that household's."""
        for subscription in list(self.subscriptions.get(household_id, ())):
            subscription.nudge()


class FakeReader:
    """The stream's read: the test's notifications, filtered by the cursor."""

    def __init__(self, pending: list[NotificationView] | None = None) -> None:
        self.pending = list(pending or [])
        self.cursors: list[NotificationId | None] = []

    async def pending_since(
        self, household_id: HouseholdId, *, since: NotificationId | None
    ) -> list[NotificationView]:
        """Record the cursor and answer the way the repository does."""
        self.cursors.append(since)
        return [
            notification
            for notification in self.pending
            if since is None or notification.notification_id.value > since.value
        ]


def a_service(
    channel: FakeChannel, reader: FakeReader, *, keep_alive: float = 5.0
) -> NotificationStreamService:
    """The real service over the test's channel and reader."""
    return NotificationStreamService(
        HouseholdSignalFanout(channel), reader, keep_alive_seconds=keep_alive
    )


@asynccontextmanager
async def open_stream(
    service: NotificationStreamService,
    *,
    household_id: HouseholdId = HOUSEHOLD,
    since: NotificationId | None = None,
) -> AsyncIterator[AsyncGenerator[NotificationStreamEvent]]:
    """Open a stream, assert it announced itself, and close it on the way out."""
    stream = service.stream(household_id, since=since)
    assert await anext(stream) == StreamOpened()
    try:
        yield stream
    finally:
        await stream.aclose()


def arrived(event: NotificationStreamEvent) -> NotificationView:
    """Unwrap an arrival, failing loudly on anything else."""
    assert isinstance(event, NotificationArrival)
    return event.notification


async def test_opening_a_stream_subscribes_to_the_household() -> None:
    channel = FakeChannel()
    service = a_service(channel, FakeReader())

    async with open_stream(service):
        assert channel.opened == 1
        assert channel.closed == 0

    assert channel.closed == 1


async def test_a_nudge_delivers_a_notification_that_arrived_during_the_wait() -> None:
    """The stream is already subscribed when it opens, so the nudge cannot be lost."""
    channel = FakeChannel()
    reader = FakeReader()
    service = a_service(channel, reader)
    notification = a_notification()

    async with open_stream(service) as stream:
        waiting = asyncio.ensure_future(anext(stream))
        reader.pending.append(notification)
        await channel.publish(HOUSEHOLD)

        assert arrived(await asyncio.wait_for(waiting, 2.0)) == notification


async def test_a_notification_already_delivered_is_not_delivered_again() -> None:
    """Unread on the server, sent on the wire: the cursor is what stops the repeat."""
    channel = FakeChannel()
    notification = a_notification()
    reader = FakeReader([notification])
    service = a_service(channel, reader, keep_alive=0.05)

    async with open_stream(service) as stream:
        assert arrived(await anext(stream)) == notification

        await channel.publish(HOUSEHOLD)
        following = await asyncio.wait_for(anext(stream), 2.0)

        assert isinstance(following, StreamKeepAlive)
        assert set(reader.cursors) == {None, notification.notification_id}


async def test_a_stream_resumes_after_the_cursor_it_is_given() -> None:
    first, second, third = a_notification(), a_notification(), a_notification()
    channel = FakeChannel()
    reader = FakeReader([first, second, third])
    service = a_service(channel, reader)

    async with open_stream(service, since=first.notification_id) as stream:
        assert arrived(await anext(stream)) == second
        assert arrived(await anext(stream)) == third

    assert reader.cursors[0] == first.notification_id


async def test_one_subscription_serves_every_open_stream_of_a_household() -> None:
    """Fan-out: the second stream of a household opens no second subscription."""
    channel = FakeChannel()
    notification = a_notification()
    reader = FakeReader([notification])
    service = a_service(channel, reader)

    async with open_stream(service) as first, open_stream(service) as second:
        assert channel.opened == 1

        await channel.publish(HOUSEHOLD)

        assert arrived(await asyncio.wait_for(anext(first), 2.0)) == notification
        assert arrived(await asyncio.wait_for(anext(second), 2.0)) == notification
        assert channel.closed == 0

    assert channel.closed == 1


async def test_a_stream_whose_signal_dies_ends_instead_of_failing() -> None:
    channel = FakeChannel()
    service = a_service(channel, FakeReader())

    async with open_stream(service) as stream:
        channel.subscriptions[HOUSEHOLD][0].fail()

        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(stream), 2.0)

    assert channel.closed == 1


async def test_a_stream_without_the_signal_degrades_to_a_plain_read() -> None:
    """No signal, but the notifications are in the database: answer and end."""
    channel = FakeChannel()
    channel.subscribe_failure = NotificationChannelError("valkey is down")
    notification = a_notification()
    reader = FakeReader([notification])
    service = a_service(channel, reader)

    stream = service.stream(HOUSEHOLD)

    assert await anext(stream) == StreamOpened(degraded=True)
    assert arrived(await anext(stream)) == notification
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    await stream.aclose()

    assert channel.opened == 0
    assert reader.cursors == [None]


async def test_the_fanout_releases_every_subscription_when_the_process_stops() -> None:
    channel = FakeChannel()
    fanout = HouseholdSignalFanout(channel)
    service = NotificationStreamService(fanout, FakeReader(), keep_alive_seconds=0.05)

    one = service.stream(HOUSEHOLD)
    other = service.stream(OTHER_HOUSEHOLD)
    assert await anext(one) == StreamOpened()
    assert await anext(other) == StreamOpened()

    await fanout.aclose()

    assert channel.closed == 2
    with pytest.raises(StopAsyncIteration):
        await anext(one)
    with pytest.raises(StopAsyncIteration):
        await anext(other)
