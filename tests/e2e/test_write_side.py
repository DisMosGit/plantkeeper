"""End-to-end test of the write side.

This is the phase's acceptance test: an HTTP command creates a plant, the outbox
row lands in the same transaction, the relay publishes it, and the event is
readable from Kafka with the contract ``docs/events.md`` promises.

The relay is driven explicitly instead of being left to run in the background: a
test that waited on a loop would be slower, and its failures would look like
flakes.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from aiokafka import AIOKafkaConsumer
from dishka import make_async_container
from faststream.kafka import KafkaBroker
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.identifiers import PlantId
from plantkeeper.domain.values import WateringInterval
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.messaging.relay import OutboxRelay
from plantkeeper.infrastructure.messaging.topics import GARDEN_EVENTS
from plantkeeper.infrastructure.persistence.models.garden import PlantModel
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel
from plantkeeper.infrastructure.persistence.repositories.care import (
    SqlAlchemyCareScheduleRepository,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker

pytestmark = pytest.mark.slow

CONSUME_TIMEOUT_MS = 20_000


async def publish_the_outbox(settings: Settings) -> None:
    """Run the relay once and fail the test if it could not publish everything."""
    container = make_async_container(*worker_providers())
    try:
        broker = await container.get(KafkaBroker)
        relay = await container.get(OutboxRelay)
        await broker.start()
        try:
            failed = await relay.run_once()
        finally:
            await broker.stop()
    finally:
        await container.close()

    assert failed == 0, "the relay could not publish every pending message"


async def consume_event(bootstrap_servers: str, topic: str, event_name: str) -> Any:
    """Return the first ``event_name`` message on ``topic``, or fail.

    The topic carries every event of its context, and this suite's earlier
    requests have already produced some of them, so the wanted message is picked
    out by its header instead of being assumed to be first.
    """
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        group_id=f"e2e-{uuid.uuid4()}",
    )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + CONSUME_TIMEOUT_MS / 1000
    await consumer.start()
    try:
        while (remaining := deadline - loop.time()) > 0:
            batches = await consumer.getmany(timeout_ms=min(int(remaining * 1000), 1_000))
            for group in batches.values():
                for message in group:
                    headers = {name: value.decode() for name, value in message.headers}
                    if headers.get("event_name") == event_name:
                        return message
    finally:
        await consumer.stop()

    pytest.fail(f"no {event_name} message arrived on {topic} within {CONSUME_TIMEOUT_MS} ms")


async def plant_count(database: str) -> int:
    """Count the plant rows."""
    engine = create_async_engine(database)
    try:
        factory = async_sessionmaker(engine)
        async with factory() as session:
            statement = select(func.count()).select_from(PlantModel)
            return int((await session.execute(statement)).scalar_one())
    finally:
        await engine.dispose()


async def outbox_rows(database: str) -> list[OutboxModel]:
    """Return every outbox row."""
    engine = create_async_engine(database)
    try:
        factory = async_sessionmaker(engine)
        async with factory() as session:
            statement = select(OutboxModel).order_by(OutboxModel.id)
            return list((await session.execute(statement)).scalars().all())
    finally:
        await engine.dispose()


async def create_household(client: AsyncClient, name: str = "Home") -> str:
    """Create a household through the API and return its identifier."""
    response = await client.post("/api/v1/households", json={"name": name})
    assert response.status_code == 201, response.text
    household_id: str = response.json()["household_id"]
    return household_id


async def test_creating_a_plant_publishes_plant_added(
    api_client: AsyncClient,
    database: str,
    kafka_bootstrap_servers: str,
) -> None:
    household_id = await create_household(api_client)
    body = {
        "household_id": household_id,
        "species_id": str(uuid.uuid4()),
        "name": "Fern",
        "location": "Shelf",
    }
    key = str(uuid.uuid4())

    created = await api_client.post("/api/v1/plants", json=body, headers={"Idempotency-Key": key})
    assert created.status_code == 201, created.text
    plant = created.json()
    assert plant["name"] == "Fern"
    assert plant["removed"] is False

    # A retry with the same key is not a second plant.
    replayed = await api_client.post("/api/v1/plants", json=body, headers={"Idempotency-Key": key})
    assert replayed.status_code == 201
    assert replayed.json() == plant

    # The same key with different content is a conflict, not a second plant.
    conflicting = await api_client.post(
        "/api/v1/plants",
        json={**body, "name": "Not a fern"},
        headers={"Idempotency-Key": key},
    )
    assert conflicting.status_code == 409
    assert conflicting.json()["error"] == "IdempotencyKeyConflictError"

    assert await plant_count(database) == 1
    pending = await outbox_rows(database)
    # Creating a household records no event (see docs/events.md), so the plant's
    # is the only row.
    assert [row.event_name for row in pending] == ["PlantAdded"]
    plant_message = pending[0]
    assert plant_message.published_at is None

    await publish_the_outbox(Settings())

    message = await consume_event(kafka_bootstrap_servers, GARDEN_EVENTS, "PlantAdded")
    headers = {name: value.decode() for name, value in message.headers}
    payload: dict[str, Any] = json.loads(message.value)

    assert headers["event_name"] == "PlantAdded"
    assert headers["event_id"] == str(plant_message.event_id)
    assert headers["event_id"] == payload["event_id"]
    assert payload["plant_id"] == plant["plant_id"]
    assert payload["household_id"] == household_id
    assert payload["name"] == "Fern"
    assert payload["location"] == "Shelf"
    assert message.key.decode() == plant["plant_id"]

    published = await outbox_rows(database)
    assert len(published) == 1
    assert published[0].published_at is not None
    assert published[0].attempts == 0
    assert published[0].last_error is None


async def test_a_plant_for_an_unknown_household_is_refused(api_client: AsyncClient) -> None:
    response = await api_client.post(
        "/api/v1/plants",
        json={
            "household_id": str(uuid.uuid4()),
            "species_id": str(uuid.uuid4()),
            "name": "Fern",
            "location": "Shelf",
        },
    )
    assert response.status_code == 404
    assert response.json()["error"] == "NotFoundError"


async def test_an_empty_location_is_unprocessable(api_client: AsyncClient) -> None:
    household_id = await create_household(api_client)
    response = await api_client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": str(uuid.uuid4()),
            "name": "Fern",
            "location": "   ",
        },
    )
    assert response.status_code == 422
    assert response.json()["error"] == "ValidationError"


async def test_watering_twice_within_an_hour_is_refused(
    api_client: AsyncClient, database: str
) -> None:
    household_id = await create_household(api_client)
    created = await api_client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": str(uuid.uuid4()),
            "name": "Fern",
            "location": "Shelf",
        },
    )
    plant_id = created.json()["plant_id"]

    # Onboarding is Phase 4's job, so the schedule is written directly here.
    engine = create_async_engine(database)
    try:
        factory = async_sessionmaker(engine)
        async with factory() as session:
            now = datetime.now(UTC)
            await SqlAlchemyCareScheduleRepository(session, AggregateTracker()).add(
                CareSchedule.create(
                    plant_id=PlantId(uuid.UUID(plant_id)),
                    watering_interval=WateringInterval(value=timedelta(days=7)),
                    starts_at=now + timedelta(days=7),
                    now=now,
                )
            )
            await session.commit()
    finally:
        await engine.dispose()

    today = await api_client.get("/api/v1/care/today", params={"household_id": household_id})
    assert today.status_code == 200
    assert today.json()["items"] == [], "a plant due in a week is not due today"

    watered = await api_client.post(f"/api/v1/care/{plant_id}/water")
    assert watered.status_code == 200, watered.text
    assert watered.json()["version"] == 2

    twice = await api_client.post(f"/api/v1/care/{plant_id}/water")
    assert twice.status_code == 409
    assert twice.json()["error"] == "PlantWateringTooSoonError"


async def test_moving_a_plant_records_only_a_real_move(api_client: AsyncClient) -> None:
    household_id = await create_household(api_client)
    created = await api_client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": str(uuid.uuid4()),
            "name": "Fern",
            "location": "Shelf",
        },
    )
    plant_id = created.json()["plant_id"]

    moved = await api_client.patch(f"/api/v1/plants/{plant_id}", json={"location": "Window"})
    assert moved.status_code == 200
    assert moved.json()["location"] == "Window"

    unchanged = await api_client.patch(f"/api/v1/plants/{plant_id}", json={"location": "Window"})
    assert unchanged.status_code == 200
    assert unchanged.json() == moved.json()

    again = await api_client.delete(f"/api/v1/plants/{plant_id}")
    assert again.status_code == 200
    assert again.json()["removed"] is True

    twice = await api_client.delete(f"/api/v1/plants/{plant_id}")
    assert twice.status_code == 409
    assert twice.json()["error"] == "PlantAlreadyRemovedError"


async def test_the_openapi_document_lists_the_write_api(api_client: AsyncClient) -> None:
    response = await api_client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]
    assert "/api/v1/plants" in paths
    assert "/api/v1/care/today" in paths
    assert "/api/v1/notifications/pending" in paths


async def test_a_sensor_is_registered_with_an_idempotency_key(api_client: AsyncClient) -> None:
    household_id = await create_household(api_client)
    created = await api_client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": str(uuid.uuid4()),
            "name": "Fern",
            "location": "Shelf",
        },
    )
    plant_id = created.json()["plant_id"]
    key = str(uuid.uuid4())

    first = await api_client.post(
        "/api/v1/sensors", json={"plant_id": plant_id}, headers={"Idempotency-Key": key}
    )
    assert first.status_code == 201, first.text

    replay = await api_client.post(
        "/api/v1/sensors", json={"plant_id": plant_id}, headers={"Idempotency-Key": key}
    )
    assert replay.status_code == 201
    assert replay.json() == first.json()

    listed = await api_client.get("/api/v1/sensors", params={"plant_id": plant_id})
    assert listed.status_code == 200
    assert len(listed.json()["items"]) == 1


async def test_a_catalogue_sync_request_is_accepted_and_published(
    api_client: AsyncClient, database: str
) -> None:
    response = await api_client.post("/api/v1/catalog/sync")
    assert response.status_code == 202
    assert response.json() == {"requested": True}

    rows = await outbox_rows(database)
    assert [row.event_name for row in rows] == ["SpeciesSyncRequested"]

    # Nothing to publish to yet in this test, but the row must be drainable.
    await publish_the_outbox(Settings())
    assert [row.published_at is not None for row in await outbox_rows(database)] == [True]
