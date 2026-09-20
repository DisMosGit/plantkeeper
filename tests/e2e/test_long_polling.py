"""End-to-end test of notification long polling.

Phase 8's acceptance test: a domain fact produces a ``Notification``, and a client
blocked in ``GET /api/v1/notifications/pending`` is woken and receives it. The
pieces are the production ones — ``register_consumers``, ``register_telemetry_ingest``,
``OutboxRelay`` and the real HTTP application — so the test exercises the wiring
rather than a test-only path.

The telemetry route is the shortest one that needs no scheduler: a dry reading goes
to ``telemetry.raw``, the ingress stores it and raises ``TelemetryReceived`` +
``SoilMoistureLow``, ``NotificationConsumer`` turns the latter into an unread
notification, and ``NotificationPusher`` nudges the household the long poll is
waiting on.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from aiokafka import AIOKafkaProducer
from dishka import make_async_container
from faststream.kafka import KafkaBroker
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from plantkeeper.domain.identifiers import HouseholdId, PlantId, SensorId
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.domain.telemetry.sensor import Sensor
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.messaging.relay import OutboxRelay
from plantkeeper.infrastructure.messaging.topics import TELEMETRY_RAW
from plantkeeper.infrastructure.persistence.models.notifications import NotificationModel
from plantkeeper.infrastructure.persistence.repositories.notifications import (
    SqlAlchemyNotificationRepository,
)
from plantkeeper.infrastructure.persistence.repositories.telemetry import (
    SqlAlchemySensorRepository,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker
from plantkeeper.workers.consumers import register_consumers, register_telemetry_ingest

pytestmark = pytest.mark.slow

POLL_TIMEOUT_SECONDS = 15.0
"""Long enough to cross the ingress, the relay, the consumer, the relay again and
the pusher; short enough that a broken pipeline fails the test instead of hanging."""

SEEDED_TIMEOUT_SECONDS = 30.0
"""A wait the test expects not to be waited at all, because a notification is ready."""


def session_factory_on(database: str) -> async_sessionmaker[AsyncSession]:
    """A session factory over the test database."""
    return async_sessionmaker(create_async_engine(database), expire_on_commit=False)


@asynccontextmanager
async def running_worker(settings: Settings) -> AsyncIterator[None]:
    """Run the worker's consumers and relay in-process.

    The registration calls are the production ones — the same two ``apps/workers``
    makes before it starts the broker — so the test exercises the wiring, not a
    test-only path. The relay runs in the background because the pipeline crosses
    it twice: the ingress's events out, and the notification's event out again.
    """
    container = make_async_container(*worker_providers())
    broker = await container.get(KafkaBroker)
    relay = await container.get(OutboxRelay)
    register_consumers(broker, container=container, settings=settings)
    register_telemetry_ingest(broker, container=container, settings=settings)
    await broker.start()
    relay_task = asyncio.create_task(relay.run())
    try:
        yield
    finally:
        relay.stop()
        relay_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await relay_task
        await broker.stop()
        await container.close()


async def publish_dry_reading(
    bootstrap_servers: str, *, sensor_id: SensorId, moisture: float = 12.0
) -> None:
    """Publish one raw dry reading, in the simulator's envelope."""
    body = json.dumps(
        {
            "sensor_id": str(sensor_id),
            "recorded_at": datetime.now(UTC).isoformat(),
            "moisture": moisture,
            "temperature": 21.0,
            "light": 800.0,
        }
    ).encode("utf-8")
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        await producer.send_and_wait(TELEMETRY_RAW, body, key=str(sensor_id).encode("utf-8"))
    finally:
        await producer.stop()


async def create_household_and_plant(client: AsyncClient) -> tuple[str, PlantId]:
    """Create a household with one plant through the API, as a client would."""
    household = await client.post("/api/v1/households", json={"name": "Home"})
    assert household.status_code == 201, household.text
    household_id = household.json()["household_id"]
    plant = await client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": str(uuid4()),
            "name": "Fern",
            "location": "Shelf",
        },
    )
    assert plant.status_code == 201, plant.text
    return household_id, PlantId(UUID(plant.json()["plant_id"]))


async def register_sensor(database: str, *, sensor_id: SensorId, plant_id: PlantId) -> None:
    """Bind a sensor to the plant, so the ingress can resolve the reading."""
    async with session_factory_on(database)() as session:
        await SqlAlchemySensorRepository(session, AggregateTracker()).add(
            Sensor(sensor_id, plant_id=plant_id, added_at=datetime.now(UTC))
        )
        await session.commit()


async def seed_notification(database: str, household_id: str) -> Notification:
    """Write one pending notification directly and return it."""
    notification = Notification.create(
        household_id=HouseholdId(UUID(household_id)),
        notification_type=NotificationType.WATERING_DUE,
        payload={"plant_id": str(uuid4())},
        now=datetime.now(UTC),
    )
    async with session_factory_on(database)() as session:
        await SqlAlchemyNotificationRepository(session, AggregateTracker()).add(notification)
        await session.commit()
    return notification


async def stored_notification_types(database: str) -> list[str]:
    """The stored notification types, oldest first."""
    async with session_factory_on(database)() as session:
        statement = select(NotificationModel).order_by(NotificationModel.created_at)
        return [row.notification_type for row in (await session.execute(statement)).scalars().all()]


async def test_a_long_poll_receives_a_notification_the_worker_created(
    api_client: AsyncClient,
    database: str,
    kafka_bootstrap_servers: str,
    worker_settings: Settings,
) -> None:
    """Dry telemetry → notification in the database → the waiting client is answered."""
    household_id, plant_id = await create_household_and_plant(api_client)
    sensor_id = SensorId(uuid4())
    await register_sensor(database, sensor_id=sensor_id, plant_id=plant_id)

    async with running_worker(worker_settings):
        poll = asyncio.create_task(
            api_client.get(
                "/api/v1/notifications/pending",
                params={"household_id": household_id, "timeout": POLL_TIMEOUT_SECONDS},
            )
        )
        # Give the poll a moment to subscribe; the endpoint re-reads after
        # subscribing and again at its deadline, so this is not a race.
        await asyncio.sleep(1.0)
        await publish_dry_reading(kafka_bootstrap_servers, sensor_id=sensor_id)
        response = await poll

    assert response.status_code == 200, response.text
    items = response.json()["items"]
    assert [item["notification_type"] for item in items] == [
        NotificationType.SOIL_MOISTURE_LOW.value
    ]
    assert items[0]["payload"]["plant_id"] == str(plant_id)
    assert items[0]["read_at"] is None
    assert await stored_notification_types(database) == [NotificationType.SOIL_MOISTURE_LOW.value]


async def test_a_long_poll_that_times_out_answers_no_content(
    api_client: AsyncClient, database: str
) -> None:
    """Nothing pending, nobody publishing: the wait ends as 204, not as an empty list."""
    household = await api_client.post("/api/v1/households", json={"name": "Quiet"})
    household_id = household.json()["household_id"]
    loop = asyncio.get_running_loop()

    started = loop.time()
    response = await api_client.get(
        "/api/v1/notifications/pending",
        params={"household_id": household_id, "timeout": 1.0},
    )
    elapsed = loop.time() - started

    assert response.status_code == 204
    assert response.content == b""
    assert elapsed >= 0.8, "the endpoint should have waited out its timeout"


async def test_a_long_poll_with_something_pending_answers_at_once(
    api_client: AsyncClient, database: str
) -> None:
    """The wait only starts when there is nothing to answer with."""
    household = await api_client.post("/api/v1/households", json={"name": "Busy"})
    household_id = household.json()["household_id"]
    notification = await seed_notification(database, household_id)
    loop = asyncio.get_running_loop()

    started = loop.time()
    response = await api_client.get(
        "/api/v1/notifications/pending",
        params={"household_id": household_id, "timeout": SEEDED_TIMEOUT_SECONDS},
    )
    elapsed = loop.time() - started

    assert response.status_code == 200
    assert [item["notification_id"] for item in response.json()["items"]] == [str(notification.id)]
    assert elapsed < 1.0, "a pending notification must not wait for the timeout"
