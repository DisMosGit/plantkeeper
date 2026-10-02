"""The household notification stream: cursor, fan-out and degradation.

The stream is the primary delivery form and the request-and-wait endpoint is its
fallback. Both are woken by the same payload-free household signal and both
answer a wake-up by reading the application's own tables, so the wire form is the
only difference: the endpoint turns the signal into one answer and ends, the
stream stays open, resumes from a cursor and keeps its own place in the
household's notifications.

Three properties are what the tests pin down:

* **The cursor belongs to the client.** Every notification the stream delivers
  moves the cursor past it, so a wake-up that reveals a notification already sent
  costs a read and never a duplicate; a client that reconnects naming its last
  notification receives what it missed.
* **One subscription per household, however many streams.** A household's open
  streams share a single presence-channel subscription, so the API process's
  Valkey connections are bounded by households rather than by open connections.
* **The signal is a latency optimisation, never the delivery.** A stream that
  cannot reach it degrades to a plain read — the pending notifications, then the
  end of the response, which an SSE client reconnects from — and a stream whose
  signal dies mid-flight ends the same way instead of failing.

No database session is held while a stream is idle: the read is a port of its own
(:class:`~plantkeeper.application.ports.notifications.PendingNotificationReader`)
whose every call opens storage and releases it again.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, Callable
from contextlib import suppress
from enum import StrEnum
from functools import partial
from typing import Final

from pydantic import BaseModel, ConfigDict

from plantkeeper.application.ports.notifications import (
    NotificationChannel,
    NotificationChannelError,
    PendingNotificationReader,
)
from plantkeeper.application.views import NotificationView
from plantkeeper.domain.identifiers import HouseholdId, NotificationId

logger = logging.getLogger(__name__)

KEEP_ALIVE_SECONDS: Final = 15.0
"""How long an idle stream waits on the signal before saying it is still there.

The same interval bounds how long a nudge lost in the window between a read and
the subscription delays delivery, and how long a stream whose client vanished is
kept alive: the stream re-reads after every wait, whatever ended it.
"""

NUDGE_WAIT_SECONDS: Final = 30.0
"""One shared subscription's wait slice.

Long enough that an idle household costs one blocked read per slice, short enough
that a cancelled fan-out is noticed without waiting for the process to stop.
"""

_STREAM_CONFIG = ConfigDict(frozen=True, extra="forbid")


class HouseholdSignalState(StrEnum):
    """What a stream's wait on the household signal ended as."""

    NUDGED = "nudged"
    """Something changed for the household: read again."""

    IDLE = "idle"
    """The deadline passed with nothing: the stream is still open."""

    LOST = "lost"
    """The signal failed: answer from the database and end the stream."""


class StreamOpened(BaseModel):
    """The stream is attached to the household's signal."""

    model_config = _STREAM_CONFIG

    degraded: bool = False
    """True when the signal was unavailable and the stream is a plain read.

    The client is told so it can treat the response as a one-shot read and
    reconnect on its own schedule instead of expecting push.
    """


class NotificationArrival(BaseModel):
    """One notification the client has not seen on this stream."""

    model_config = _STREAM_CONFIG

    notification: NotificationView


class StreamKeepAlive(BaseModel):
    """Nothing happened; the stream is still there."""

    model_config = _STREAM_CONFIG


type NotificationStreamEvent = StreamOpened | NotificationArrival | StreamKeepAlive
"""What a stream yields, in the application's own vocabulary.

The SSE frames, the retry hint and the keep-alive comments are the HTTP layer's
translation of these; nothing above formats a wire protocol.
"""


class HouseholdSignal:
    """One open stream's handle on its household's shared subscription."""

    def __init__(self, fanout: _HouseholdFanout) -> None:
        self._fanout = fanout
        self._wakeup = asyncio.Event()
        self._lost = False
        self._released = False

    async def wait(self, timeout: float) -> HouseholdSignalState:
        """Wait for a nudge, for the deadline, or for the signal to fail."""
        if self._lost:
            return HouseholdSignalState.LOST
        try:
            async with asyncio.timeout(timeout):
                await self._wakeup.wait()
        except TimeoutError:
            return HouseholdSignalState.IDLE
        self._wakeup.clear()
        return HouseholdSignalState.LOST if self._lost else HouseholdSignalState.NUDGED

    async def release(self) -> None:
        """Let go of the signal; the last stream out closes the subscription."""
        if self._released:
            return
        self._released = True
        await self._fanout.detach(self)

    def _nudge(self) -> None:
        """Wake this stream because the household changed."""
        self._wakeup.set()

    def _lose(self) -> None:
        """Wake this stream because the signal is gone."""
        self._lost = True
        self._wakeup.set()


class _HouseholdFanout:
    """The one subscription a household's open streams share."""

    def __init__(
        self,
        channel: NotificationChannel,
        household_id: HouseholdId,
        *,
        on_lost: Callable[[_HouseholdFanout], None],
    ) -> None:
        self._channel = channel
        self._household_id = household_id
        self._on_lost = on_lost
        self._signals: set[HouseholdSignal] = set()
        self._task: asyncio.Task[None] | None = None
        self._ready = asyncio.Event()
        self._failure: NotificationChannelError | None = None

    def attach(self) -> HouseholdSignal:
        """Register one stream, opening the shared subscription if it is the first."""
        signal = HouseholdSignal(self)
        self._signals.add(signal)
        if self._task is None:
            self._task = asyncio.create_task(self._pump())
        return signal

    async def ready(self) -> None:
        """Wait until the subscription is open, or raise why it is not."""
        await self._ready.wait()
        if self._failure is not None:
            raise self._failure

    async def detach(self, signal: HouseholdSignal) -> None:
        """Drop one stream; the last one out cancels the shared subscription."""
        self._signals.discard(signal)
        if self._signals or self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def close(self) -> None:
        """End every stream of this household and close the subscription with them.

        The streams are told the signal is gone before they are detached: a
        process that is stopping has no channel left to wake them, and a stream
        that kept waiting would only be closed by its own transport.
        """
        for signal in list(self._signals):
            signal._lose()
        for signal in list(self._signals):
            await signal.release()

    async def _pump(self) -> None:
        """Read nudges off the household's channel and wake every open stream."""
        try:
            async with self._channel.subscribe(self._household_id) as subscription:
                self._ready.set()
                while True:
                    if await subscription.wait(NUDGE_WAIT_SECONDS):
                        for signal in list(self._signals):
                            signal._nudge()
        except NotificationChannelError as exc:
            logger.warning(
                "notification signal lost for household %s: %s", self._household_id.value, exc
            )
            self._failure = exc
            self._ready.set()
            for signal in list(self._signals):
                signal._lose()
            self._on_lost(self)


class HouseholdSignalFanout:
    """One presence-channel subscription per household, shared by its streams."""

    def __init__(self, channel: NotificationChannel) -> None:
        self._channel = channel
        self._households: dict[HouseholdId, _HouseholdFanout] = {}
        self._lock = asyncio.Lock()

    async def attach(self, household_id: HouseholdId) -> HouseholdSignal:
        """Attach one stream, subscribing to the household if it is the first.

        Raises :class:`NotificationChannelError` when the signal cannot be
        reached: the caller is expected to degrade to a plain read, because the
        notifications are in the database either way.
        """
        async with self._lock:
            fanout = self._households.get(household_id)
            if fanout is None:
                fanout = _HouseholdFanout(
                    self._channel, household_id, on_lost=partial(self._forget, household_id)
                )
                self._households[household_id] = fanout
            signal = fanout.attach()
        try:
            await fanout.ready()
        except NotificationChannelError:
            await signal.release()
            raise
        return signal

    async def aclose(self) -> None:
        """Release every subscription and open stream; the process is stopping."""
        for fanout in list(self._households.values()):
            await fanout.close()
        self._households.clear()

    def _forget(self, household_id: HouseholdId, fanout: _HouseholdFanout) -> None:
        """Drop a fan-out whose subscription failed, so the next stream retries it."""
        if self._households.get(household_id) is fanout:
            del self._households[household_id]


class NotificationStreamService:
    """Stream a household's notifications, resuming from a client's cursor."""

    def __init__(
        self,
        fanout: HouseholdSignalFanout,
        reader: PendingNotificationReader,
        *,
        keep_alive_seconds: float = KEEP_ALIVE_SECONDS,
    ) -> None:
        self._fanout = fanout
        self._reader = reader
        self._keep_alive_seconds = keep_alive_seconds

    async def stream(
        self, household_id: HouseholdId, *, since: NotificationId | None = None
    ) -> AsyncGenerator[NotificationStreamEvent]:
        """Yield the household's notifications as they appear, after ``since``.

        The first event says whether the signal was reached; then every wake-up
        re-reads the database and yields what the cursor has not passed yet.
        Closing the generator — a client that disconnected, a process that is
        stopping — releases this stream's share of the subscription.
        """
        cursor = since
        try:
            signal = await self._fanout.attach(household_id)
        except NotificationChannelError as exc:
            logger.warning(
                "notification signal unavailable for household %s, answering from the database: %s",
                household_id.value,
                exc,
            )
            yield StreamOpened(degraded=True)
            async for arrival in self._drain(household_id, cursor):
                cursor = arrival.notification.notification_id
                yield arrival
            return

        try:
            yield StreamOpened()
            while True:
                async for arrival in self._drain(household_id, cursor):
                    cursor = arrival.notification.notification_id
                    yield arrival
                match await signal.wait(self._keep_alive_seconds):
                    case HouseholdSignalState.NUDGED:
                        continue
                    case HouseholdSignalState.IDLE:
                        yield StreamKeepAlive()
                    case HouseholdSignalState.LOST:
                        # The subscription is gone, but the notifications are not:
                        # deliver what a final read finds and end the response, so
                        # the client reconnects into the plain-read fallback.
                        async for arrival in self._drain(household_id, cursor):
                            yield arrival
                        return
        finally:
            await signal.release()

    async def _drain(
        self, household_id: HouseholdId, since: NotificationId | None
    ) -> AsyncGenerator[NotificationArrival]:
        """Read once and yield everything the cursor has not passed yet."""
        for notification in await self._reader.pending_since(household_id, since=since):
            yield NotificationArrival(notification=notification)
