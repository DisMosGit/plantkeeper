"""Where a simulated reading goes.

Two implementations, one protocol:

* :class:`KafkaPublisher` sends to ``telemetry.raw`` through ``aiokafka``, in
  batches of :data:`BATCH_MAX_MESSAGES` or once :data:`BATCH_MAX_SECONDS` has
  passed, whichever comes first — a twenty-sensor stream is twenty small
  messages every ten seconds, and one round trip per message would be waste;
* :class:`JournalPublisher` writes the same JSON to any text stream, one object
  per line, which is what ``--dry-run`` prints and what ``--replay`` reads back.

The protocol exists so the simulation loop can be driven by a fake in a unit
test: neither implementation is needed to check that the timetable is right.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Final, Protocol, TextIO

from aiokafka import AIOKafkaProducer

from plantkeeper.iot_simulator.simulator import JSON_TOPIC, Reading

BATCH_MAX_MESSAGES: Final = 100
"""How many readings are queued before a flush, when the clock allows it."""

BATCH_MAX_SECONDS: Final = 1.0
"""How long a partial batch waits before it is sent anyway."""

DEFAULT_REQUEST_TIMEOUT_MS: Final = 30_000


class Publisher(Protocol):
    """Somewhere a reading can be sent."""

    async def send(self, reading: Reading) -> None:
        """Hand one reading over for delivery."""
        ...

    async def flush(self) -> None:
        """Deliver everything handed over so far."""
        ...

    async def stop(self) -> None:
        """Flush and release whatever the publisher holds."""
        ...


class JournalPublisher:
    """Writes readings as JSON lines, without a broker.

    Used for ``--dry-run`` (to stdout) and by the unit tests (to a buffer). The
    output is byte-for-byte what ``--replay`` reads, so a dry run is a recording.
    """

    def __init__(self, stream: TextIO) -> None:
        self._stream = stream

    async def send(self, reading: Reading) -> None:
        """Append one JSON line to the stream."""
        self._stream.write(reading.to_json() + "\n")

    async def flush(self) -> None:
        """Flush the underlying stream, if it buffers."""
        self._stream.flush()

    async def stop(self) -> None:
        """Flush; the stream itself belongs to the caller."""
        await self.flush()


class KafkaPublisher:
    """Batches readings into Kafka's ``telemetry.raw`` topic."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str = JSON_TOPIC,
        batch_max_messages: int = BATCH_MAX_MESSAGES,
        batch_max_seconds: float = BATCH_MAX_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if batch_max_messages < 1:
            raise ValueError("a batch holds at least one message")
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id="plantkeeper-iot-simulator",
            acks="all",
            enable_idempotence=True,
            linger_ms=50,
            request_timeout_ms=DEFAULT_REQUEST_TIMEOUT_MS,
        )
        self._topic = topic
        self._batch_max_messages = batch_max_messages
        self._batch_max_seconds = batch_max_seconds
        self._clock = clock
        self._pending: list[Awaitable[object]] = []
        self._first_pending_at: float | None = None
        self._started = False

    @property
    def topic(self) -> str:
        """The topic readings are sent to."""
        return self._topic

    async def start(self) -> None:
        """Connect the producer. Idempotent, so a caller may start it twice."""
        if self._started:
            return
        await self._producer.start()
        self._started = True

    async def send(self, reading: Reading) -> None:
        """Queue one reading, flushing the batch when either bound is reached.

        The future the producer hands back is collected rather than awaited here.
        Awaiting it would deliver each reading before the next one is queued, and
        then the batch could never hold more than a single record; the futures are
        awaited together in :meth:`flush`.
        """
        future = await self._producer.send(
            self._topic,
            reading.to_json().encode("utf-8"),
            key=str(reading.sensor_id).encode("utf-8"),
        )
        self._pending.append(future)
        if self._first_pending_at is None:
            self._first_pending_at = self._clock()
        if (
            len(self._pending) >= self._batch_max_messages
            or self._clock() - self._first_pending_at >= self._batch_max_seconds
        ):
            await self.flush()

    async def flush(self) -> None:
        """Wait until every queued reading has been acknowledged.

        The batch is cleared before it is awaited: a delivery error is raised to
        the caller rather than silently retried, and the readings that did arrive
        are not sent a second time by the next flush.
        """
        pending, self._pending = self._pending, []
        self._first_pending_at = None
        if pending:
            await asyncio.gather(*pending)
        await self._producer.flush()

    async def stop(self) -> None:
        """Flush and close the producer, so no reading is lost on shutdown."""
        if not self._started:
            return
        await self.flush()
        await self._producer.stop()
        self._started = False


def build_publisher(
    *,
    dry_run: bool,
    bootstrap_servers: str,
    topic: str = JSON_TOPIC,
    stream: TextIO,
) -> Publisher:
    """Return the publisher a run with these options should use."""
    if dry_run:
        return JournalPublisher(stream)
    return KafkaPublisher(bootstrap_servers=bootstrap_servers, topic=topic)
