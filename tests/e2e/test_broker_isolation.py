"""The broker between tests: what one test publishes, the next one is not offered.

Two tests, in this order — pytest runs a module's tests in the order they are
defined. The first publishes a plant, in the envelope the relay writes; the
second starts a consumer group of its own over the same topic.

Nothing about that group keeps the first test's message away on its own: it is
fresh, and it resets to ``earliest``, so over a broker that still held the
message it would read it exactly as the suite used to. Only the isolation fixture
that every consumer-starting harness depends on empties the topic first. The
second test asserts both halves of that: none of the earlier plant is offered,
and the group is not simply broken — its own plant arrives.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from plantkeeper.domain.garden.events import PlantAdded
from plantkeeper.infrastructure.messaging.topics import (
    GARDEN_EVENTS,
    HEADER_EVENT_ID,
    HEADER_EVENT_NAME,
    partition_key_for,
)

pytestmark = pytest.mark.slow

NOTHING_OFFERED_TIMEOUT_MS = 3_000
"""How long an empty topic is given to prove it is empty.

Long enough to cover a fetch that finds nothing — a consumer that is offered a
record gets it within one round trip — and short enough not to slow the suite.
"""

RECORD_TIMEOUT_MS = 20_000
"""How long the test's own record is given to be offered before the test fails."""


def a_plant() -> PlantAdded:
    """One plant-added event, in the shape the aggregate raises it."""
    now = datetime.now(UTC)
    return PlantAdded.model_validate(
        {
            "plant_id": str(uuid4()),
            "household_id": str(uuid4()),
            "species_id": str(uuid4()),
            "name": "Fern",
            "location": "Shelf",
            "added_at": now.isoformat(),
            "occurred_at": now.isoformat(),
        }
    )


async def publish(bootstrap_servers: str, event: PlantAdded) -> None:
    """Publish an event, in the envelope the outbox relay writes (``docs/events.md``)."""
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        await producer.send_and_wait(
            GARDEN_EVENTS,
            event.model_dump_json().encode("utf-8"),
            key=partition_key_for(event).encode("utf-8"),
            headers=[
                (HEADER_EVENT_NAME, type(event).__name__.encode("utf-8")),
                (HEADER_EVENT_ID, str(event.event_id).encode("utf-8")),
            ],
        )
    finally:
        await producer.stop()


@asynccontextmanager
async def fresh_group(bootstrap_servers: str) -> AsyncIterator[AIOKafkaConsumer]:
    """A consumer group that has never read this topic, reset to its beginning."""
    consumer = AIOKafkaConsumer(
        GARDEN_EVENTS,
        bootstrap_servers=bootstrap_servers,
        group_id=f"e2e-isolation-{uuid4()}",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.stop()


async def offered(
    consumer: AIOKafkaConsumer, *, timeout_ms: int, until: int
) -> list[tuple[str, str]]:
    """The ``(event_name, event_id)`` of every record offered until ``until`` of them.

    Stops at the first record when ``until`` is one, so the caller asserts on what
    the broker offered *first*: a leftover from an earlier test would be that
    record, and the assertion on its identity is what fails.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_ms / 1000
    records: list[tuple[str, str]] = []
    while len(records) < until and (remaining := deadline - loop.time()) > 0:
        batches = await consumer.getmany(timeout_ms=min(int(remaining * 1000), 500))
        for group in batches.values():
            for message in group:
                headers = {name: value.decode("utf-8") for name, value in message.headers}
                records.append((headers[HEADER_EVENT_NAME], headers[HEADER_EVENT_ID]))
    return records


async def test_a_plant_published_before_the_next_test(
    kafka_bootstrap_servers: str, isolated_broker: None
) -> None:
    """Publish a plant, and leave it on the topic for the next test to not be offered."""
    plant = a_plant()
    await publish(kafka_bootstrap_servers, plant)

    async with fresh_group(kafka_bootstrap_servers) as consumer:
        assert await offered(consumer, timeout_ms=RECORD_TIMEOUT_MS, until=1) == [
            ("PlantAdded", str(plant.event_id))
        ], "the topic should carry the plant this test published, and only it"


async def test_a_fresh_group_is_offered_nothing_from_the_test_before(
    kafka_bootstrap_servers: str, isolated_broker: None
) -> None:
    """The previous test's plant is still on the broker; this group must not see it."""
    async with fresh_group(kafka_bootstrap_servers) as consumer:
        assert await offered(consumer, timeout_ms=NOTHING_OFFERED_TIMEOUT_MS, until=1) == [], (
            "an earlier test's plant was offered to this test's consumer group"
        )

        # The group is live rather than merely silent: what this test publishes
        # itself does reach it.
        mine = a_plant()
        await publish(kafka_bootstrap_servers, mine)

        assert await offered(consumer, timeout_ms=RECORD_TIMEOUT_MS, until=1) == [
            ("PlantAdded", str(mine.event_id))
        ]
