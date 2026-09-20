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
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from aiokafka import AIOKafkaConsumer
from dishka import make_async_container
from faststream.kafka import KafkaBroker
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import HouseholdId, PlantId
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType
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
from plantkeeper.infrastructure.persistence.repositories.catalog import (
    SqlAlchemySpeciesRepository,
)
from plantkeeper.infrastructure.persistence.repositories.notifications import (
    SqlAlchemyNotificationRepository,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker

pytestmark = pytest.mark.slow

CONSUME_TIMEOUT_MS = 20_000

EXPECTED_API_SURFACE: dict[str, set[str]] = {
    "/api/v1/plants": {"get", "post"},
    "/api/v1/plants/{plant_id}": {"delete", "get", "patch"},
    "/api/v1/households": {"post"},
    "/api/v1/households/{household_id}": {"get"},
    "/api/v1/care/today": {"get"},
    "/api/v1/care/{plant_id}/water": {"post"},
    "/api/v1/care/{plant_id}/skip": {"post"},
    "/api/v1/sensors": {"get", "post"},
    "/api/v1/sensors/{sensor_id}": {"delete"},
    "/api/v1/catalog/species": {"get"},
    "/api/v1/catalog/species/{species_id}": {"get"},
    "/api/v1/catalog/sync": {"post"},
    "/api/v1/journal/{plant_id}": {"get"},
    "/api/v1/journal/{plant_id}/at": {"get"},
    "/api/v1/notifications/pending": {"get"},
    "/api/v1/notifications/{notification_id}/ack": {"post"},
}
"""Every operation the write API exposes, as a contract rather than a sample."""


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


async def consume_event(bootstrap_servers: str, topic: str, event_name: str, event_id: str) -> Any:
    """Return the ``event_id`` message of ``event_name`` on ``topic``, or fail.

    The topic carries every event of its context, and the suite shares one broker
    with the read-side tests, so earlier tests have already published messages of
    the same name. The wanted one is picked out by its ``event_id`` header instead
    of being assumed to be first.
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
                    if (
                        headers.get("event_name") == event_name
                        and headers.get("event_id") == event_id
                    ):
                        return message
    finally:
        await consumer.stop()

    pytest.fail(
        f"no {event_name} ({event_id}) message arrived on {topic} within {CONSUME_TIMEOUT_MS} ms"
    )


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


async def create_plant(
    client: AsyncClient,
    household_id: str,
    *,
    name: str = "Fern",
    location: str = "Shelf",
) -> dict[str, Any]:
    """Create a plant through the API and return its response body."""
    response = await client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": str(uuid.uuid4()),
            "name": name,
            "location": location,
        },
    )
    assert response.status_code == 201, response.text
    body: dict[str, Any] = response.json()
    return body


@asynccontextmanager
async def session_on(database: str) -> AsyncIterator[AsyncSession]:
    """A session on the test database, with its engine closed afterwards.

    Rows the write side cannot create yet — a care schedule, a catalogue entry, a
    notification — are seeded through the repositories with this, exactly as the
    saga or consumer that will own them later would.
    """
    engine = create_async_engine(database)
    try:
        async with async_sessionmaker(engine)() as session:
            yield session
    finally:
        await engine.dispose()


async def seed_care_schedule(database: str, plant_id: str) -> datetime:
    """Write a care schedule directly and return when its first watering is due."""
    now = datetime.now(UTC)
    starts_at = now + timedelta(days=7)
    async with session_on(database) as session:
        await SqlAlchemyCareScheduleRepository(session, AggregateTracker()).add(
            CareSchedule.create(
                plant_id=PlantId(uuid.UUID(plant_id)),
                watering_interval=WateringInterval(value=timedelta(days=7)),
                starts_at=starts_at,
                now=now,
            )
        )
        await session.commit()
    return starts_at


async def seed_species(database: str) -> Species:
    """Write one catalogue entry directly and return it."""
    species = Species.create(
        scientific_name="Nephrolepis exaltata",
        common_name="Boston fern",
        watering_interval=WateringInterval(value=timedelta(days=7)),
        light_requirement=LightRequirement.MEDIUM,
    )
    async with session_on(database) as session:
        await SqlAlchemySpeciesRepository(session, AggregateTracker()).add(species)
        await session.commit()
    return species


async def seed_notification(database: str, household_id: str) -> Notification:
    """Write one pending notification directly and return it."""
    notification = Notification.create(
        household_id=HouseholdId(uuid.UUID(household_id)),
        notification_type=NotificationType.WATERING_DUE,
        payload={"plant_id": str(uuid.uuid4())},
        now=datetime.now(UTC),
    )
    async with session_on(database) as session:
        await SqlAlchemyNotificationRepository(session, AggregateTracker()).add(notification)
        await session.commit()
    return notification


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

    message = await consume_event(
        kafka_bootstrap_servers, GARDEN_EVENTS, "PlantAdded", str(plant_message.event_id)
    )
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
    await seed_care_schedule(database, plant_id)

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
    paths: dict[str, dict[str, object]] = response.json()["paths"]

    documented = {path: set(operations) for path, operations in paths.items()}
    assert documented == EXPECTED_API_SURFACE


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


async def test_a_plant_can_be_read_back(api_client: AsyncClient) -> None:
    household_id = await create_household(api_client)
    fern = await create_plant(api_client, household_id)
    monstera = await create_plant(api_client, household_id, name="Monstera", location="Window")

    listed = await api_client.get("/api/v1/plants", params={"household_id": household_id})
    assert listed.status_code == 200
    # Oldest first, so the order the plants were added in is the order they read back.
    assert [item["name"] for item in listed.json()["items"]] == ["Fern", "Monstera"]

    one = await api_client.get(f"/api/v1/plants/{monstera['plant_id']}")
    assert one.status_code == 200
    assert one.json() == monstera

    unknown = await api_client.get(f"/api/v1/plants/{uuid.uuid4()}")
    assert unknown.status_code == 404
    assert unknown.json()["error"] == "NotFoundError"

    removed = await api_client.delete(f"/api/v1/plants/{fern['plant_id']}")
    assert removed.status_code == 200

    still_listed = await api_client.get("/api/v1/plants", params={"household_id": household_id})
    assert [item["name"] for item in still_listed.json()["items"]] == ["Monstera"]

    with_removed = await api_client.get(
        "/api/v1/plants",
        params={"household_id": household_id, "include_removed": "true"},
    )
    assert [item["name"] for item in with_removed.json()["items"]] == ["Fern", "Monstera"]


async def test_a_household_reports_how_many_plants_it_owns(api_client: AsyncClient) -> None:
    household_id = await create_household(api_client, name="Home")
    empty = await api_client.get(f"/api/v1/households/{household_id}")
    assert empty.status_code == 200
    assert empty.json() == {"household_id": household_id, "name": "Home", "plant_count": 0}

    await create_plant(api_client, household_id)

    occupied = await api_client.get(f"/api/v1/households/{household_id}")
    assert occupied.status_code == 200
    assert occupied.json()["plant_count"] == 1

    unknown = await api_client.get(f"/api/v1/households/{uuid.uuid4()}")
    assert unknown.status_code == 404
    assert unknown.json()["error"] == "NotFoundError"


async def test_a_watering_can_be_skipped(api_client: AsyncClient, database: str) -> None:
    household_id = await create_household(api_client)
    plant = await create_plant(api_client, household_id)
    plant_id = plant["plant_id"]
    due_at = await seed_care_schedule(database, plant_id)

    skipped = await api_client.post(f"/api/v1/care/{plant_id}/skip")
    assert skipped.status_code == 200, skipped.text
    body = skipped.json()
    assert body["version"] == 2
    # Skipping moves the schedule one whole interval on, without watering.
    assert datetime.fromisoformat(body["next_watering_at"]) == due_at + timedelta(days=7)

    unknown = await api_client.post(f"/api/v1/care/{uuid.uuid4()}/skip")
    assert unknown.status_code == 404
    assert unknown.json()["error"] == "NotFoundError"


async def test_a_sensor_can_be_unregistered(api_client: AsyncClient) -> None:
    household_id = await create_household(api_client)
    plant = await create_plant(api_client, household_id)

    registered = await api_client.post("/api/v1/sensors", json={"plant_id": plant["plant_id"]})
    assert registered.status_code == 201, registered.text
    sensor_id = registered.json()["sensor_id"]

    deleted = await api_client.delete(f"/api/v1/sensors/{sensor_id}")
    assert deleted.status_code == 200
    assert deleted.json()["sensor_id"] == sensor_id

    listed = await api_client.get("/api/v1/sensors", params={"plant_id": plant["plant_id"]})
    assert listed.status_code == 200
    assert listed.json()["items"] == []

    unknown = await api_client.delete(f"/api/v1/sensors/{uuid.uuid4()}")
    assert unknown.status_code == 404
    assert unknown.json()["error"] == "NotFoundError"


async def test_the_catalogue_can_be_listed_and_read(api_client: AsyncClient, database: str) -> None:
    species = await seed_species(database)

    listed = await api_client.get("/api/v1/catalog/species")
    assert listed.status_code == 200
    assert [item["species_id"] for item in listed.json()["items"]] == [str(species.id)]

    one = await api_client.get(f"/api/v1/catalog/species/{species.id}")
    assert one.status_code == 200
    assert one.json()["common_name"] == "Boston fern"
    assert one.json()["light_requirement"] == "medium"
    assert one.json()["version"] == 1

    unknown = await api_client.get(f"/api/v1/catalog/species/{uuid.uuid4()}")
    assert unknown.status_code == 404
    assert unknown.json()["error"] == "NotFoundError"


async def test_pending_notifications_are_listed_and_acknowledged(
    api_client: AsyncClient, database: str
) -> None:
    household_id = await create_household(api_client)
    notification = await seed_notification(database, household_id)
    notification_id = str(notification.id)

    pending = await api_client.get(
        "/api/v1/notifications/pending", params={"household_id": household_id}
    )
    assert pending.status_code == 200
    assert [item["notification_id"] for item in pending.json()["items"]] == [notification_id]

    acknowledged = await api_client.post(f"/api/v1/notifications/{notification_id}/ack")
    assert acknowledged.status_code == 200, acknowledged.text
    assert acknowledged.json()["read_at"] is not None

    after = await api_client.get(
        "/api/v1/notifications/pending", params={"household_id": household_id}
    )
    assert after.status_code == 200
    assert after.json()["items"] == []

    twice = await api_client.post(f"/api/v1/notifications/{notification_id}/ack")
    assert twice.status_code == 409
    assert twice.json()["error"] == "NotificationAlreadyReadError"

    unknown = await api_client.post(f"/api/v1/notifications/{uuid.uuid4()}/ack")
    assert unknown.status_code == 404
    assert unknown.json()["error"] == "NotFoundError"
