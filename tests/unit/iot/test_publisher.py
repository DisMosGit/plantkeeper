"""The two publishers, without a broker and without a clock.

``KafkaPublisher`` is tested through a fake ``AIOKafkaProducer``: the interesting
behaviour is when a batch is flushed, and that is decided by two counters the test
drives by hand. ``JournalPublisher`` needs no fake at all — it writes to a buffer.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from io import StringIO
from typing import Any, ClassVar
from uuid import UUID

import pytest

from plantkeeper.iot_simulator.publisher import (
    BATCH_MAX_MESSAGES,
    JournalPublisher,
    KafkaPublisher,
    build_publisher,
)
from plantkeeper.iot_simulator.simulator import Reading

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
SENSOR_ID = UUID("00000000-0000-0000-0000-0000000000bb")


def a_reading(moisture: float = 42.5) -> Reading:
    """One reading with a known payload."""
    return Reading(
        sensor_id=SENSOR_ID,
        recorded_at=NOW,
        moisture=moisture,
        temperature=21.5,
        light=1200.0,
    )


class FakeProducer:
    """Stands in for ``AIOKafkaProducer`` and counts what it was asked to do.

    ``send`` hands back a future the way ``aiokafka`` does. By default it is
    already resolved, so a flush completes immediately; ``deferred`` leaves it
    pending until :meth:`release`, which is what lets a test show that the
    publisher queues readings instead of waiting for them one at a time.
    """

    instances: ClassVar[list[FakeProducer]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.pending: list[asyncio.Future[None]] = []
        self.flushes = 0
        self.started = 0
        self.stopped = 0
        self.deferred = False
        self.delivery_error: Exception | None = None
        FakeProducer.instances.append(self)

    async def start(self) -> None:
        """Record the connection."""
        self.started += 1

    async def send(self, topic: str, value: bytes, key: bytes) -> asyncio.Future[None]:
        """Register one message and return the future its delivery completes."""
        self.sent.append((topic, value, key))
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if self.delivery_error is not None:
            future.set_exception(self.delivery_error)
        elif self.deferred:
            self.pending.append(future)
        else:
            future.set_result(None)
        return future

    def release(self) -> None:
        """Acknowledge every deferred delivery."""
        pending, self.pending = self.pending, []
        for future in pending:
            future.set_result(None)

    async def flush(self) -> None:
        """Record a flush."""
        self.flushes += 1

    async def stop(self) -> None:
        """Record the close."""
        self.stopped += 1


@pytest.fixture
def fake_producer(monkeypatch: pytest.MonkeyPatch) -> type[FakeProducer]:
    """Replace ``aiokafka``'s producer with the fake, resetting its instances."""
    FakeProducer.instances.clear()
    monkeypatch.setattr("plantkeeper.iot_simulator.publisher.AIOKafkaProducer", FakeProducer)
    return FakeProducer


def build(fake: type[FakeProducer], **kwargs: Any) -> tuple[KafkaPublisher, FakeProducer]:
    """Build a publisher over the fake and return both it and the producer."""
    publisher = KafkaPublisher(bootstrap_servers="broker:9092", **kwargs)
    return publisher, fake.instances[0]


async def test_a_full_batch_is_flushed_immediately(fake_producer: type[FakeProducer]) -> None:
    publisher, fake = build(fake_producer, batch_max_messages=3)

    for _ in range(3):
        await publisher.send(a_reading())

    assert len(fake.sent) == 3
    assert fake.flushes == 1


async def test_a_partial_batch_is_flushed_once_time_has_passed(
    fake_producer: type[FakeProducer],
) -> None:
    now = [100.0]
    publisher, fake = build(
        fake_producer, batch_max_messages=100, batch_max_seconds=1.0, clock=lambda: now[0]
    )

    await publisher.send(a_reading())
    assert fake.flushes == 0, "one message does not fill the batch"

    now[0] += 2.0
    await publisher.send(a_reading())

    assert fake.flushes == 1


async def test_the_default_batch_is_the_one_the_roadmap_asks_for() -> None:
    assert BATCH_MAX_MESSAGES == 100


async def test_readings_are_batched_rather_than_delivered_one_at_a_time(
    fake_producer: type[FakeProducer],
) -> None:
    """``send`` queues a record instead of waiting for the broker to release it.

    If the publisher awaited each delivery, the second ``send`` would block on the
    first unresolved future, and a batch could never hold two records.
    """
    publisher, fake = build(fake_producer, batch_max_messages=100)
    fake.deferred = True

    await asyncio.wait_for(publisher.send(a_reading()), timeout=1.0)
    await asyncio.wait_for(publisher.send(a_reading()), timeout=1.0)

    assert len(fake.pending) == 2, "both readings are in flight before either lands"

    fake.release()
    await publisher.flush()
    assert len(fake.sent) == 2


async def test_a_delivery_failure_is_raised_by_the_flush(
    fake_producer: type[FakeProducer],
) -> None:
    """A record the broker refused must fail the run, not vanish inside a batch."""
    publisher, fake = build(fake_producer, batch_max_messages=100)
    fake.delivery_error = RuntimeError("the broker refused the record")

    await publisher.send(a_reading())

    with pytest.raises(RuntimeError, match="refused the record"):
        await publisher.flush()


async def test_every_reading_is_keyed_by_its_sensor(fake_producer: type[FakeProducer]) -> None:
    publisher, fake = build(fake_producer)
    await publisher.start()

    await publisher.send(a_reading())
    await publisher.flush()

    topic, value, key = fake.sent[0]
    assert topic == "telemetry.raw"
    assert key.decode() == str(SENSOR_ID)
    assert json.loads(value)["sensor_id"] == str(SENSOR_ID)
    assert publisher.topic == "telemetry.raw"


async def test_stopping_flushes_what_is_queued_and_closes_the_producer(
    fake_producer: type[FakeProducer],
) -> None:
    publisher, fake = build(fake_producer, batch_max_messages=100)

    await publisher.start()
    await publisher.send(a_reading())
    await publisher.stop()

    assert fake.started == 1
    assert fake.flushes == 1
    assert fake.stopped == 1

    # Stopping twice must not close a producer twice.
    await publisher.stop()
    assert fake.stopped == 1


def test_a_batch_holds_at_least_one_message() -> None:
    with pytest.raises(ValueError, match="at least one message"):
        KafkaPublisher(bootstrap_servers="broker:9092", batch_max_messages=0)


async def test_the_journal_publisher_writes_one_json_object_per_line() -> None:
    stream = StringIO()
    publisher = JournalPublisher(stream)

    await publisher.send(a_reading(moisture=1.0))
    await publisher.send(a_reading(moisture=99.0))
    await publisher.flush()

    lines = stream.getvalue().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["moisture"] == 1.0
    assert json.loads(lines[1])["moisture"] == 99.0
    assert json.loads(lines[0])["recorded_at"].startswith("2026-09-20T12:00:00")


def test_a_dry_run_builds_a_journal_and_a_live_run_a_kafka_publisher(
    fake_producer: type[FakeProducer],
) -> None:
    stream = StringIO()

    assert isinstance(
        build_publisher(dry_run=True, bootstrap_servers="broker:9092", stream=stream),
        JournalPublisher,
    )
    assert isinstance(
        build_publisher(dry_run=False, bootstrap_servers="broker:9092", stream=stream),
        KafkaPublisher,
    )
