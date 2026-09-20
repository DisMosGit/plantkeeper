"""End-to-end test of the onboarding saga.

This is Phase 4's acceptance test: ``POST /api/v1/plants`` writes ``PlantAdded`` to
the outbox, the relay publishes it, the worker's real consumer registration feeds
it to ``OnboardPlantSaga`` over a real broker, and the saga's steps leave a care
schedule, a notification and a published ``PlantOnboarded`` behind.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from dishka import make_async_container
from faststream.kafka import KafkaBroker
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import PlantId
from plantkeeper.domain.values import WateringInterval
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.messaging.relay import OutboxRelay
from plantkeeper.infrastructure.persistence.models.notifications import NotificationModel
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel
from plantkeeper.infrastructure.persistence.repositories.care import (
    SqlAlchemyCareScheduleRepository,
)
from plantkeeper.infrastructure.persistence.repositories.catalog import (
    SqlAlchemySpeciesRepository,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker
from plantkeeper.workers.consumers import register_consumers

pytestmark = pytest.mark.slow

SAGA_TIMEOUT_SECONDS = 30.0
POLL_INTERVAL_SECONDS = 0.25
WEEK = timedelta(days=7)


async def session_factory_on(database: str) -> async_sessionmaker[AsyncSession]:
    """A session factory on the test database."""
    return async_sessionmaker(create_async_engine(database), expire_on_commit=False)


@asynccontextmanager
async def running_worker_consumers(settings: Settings) -> AsyncIterator[None]:
    """Run the worker's saga consumers in-process, as ``make workers`` would.

    The registration function is the production one — the same call
    ``apps/workers`` makes before it starts the broker — so this exercises the
    wiring, not a test-only path.
    """
    container = make_async_container(*worker_providers())
    broker = await container.get(KafkaBroker)
    register_consumers(broker, container=container, settings=settings)
    await broker.start()
    try:
        yield
    finally:
        await broker.stop()
        await container.close()


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


async def seed_species(database: str) -> Species:
    """Write one catalogue entry the onboarding saga can resolve."""
    species = Species.create(
        scientific_name="Nephrolepis exaltata",
        common_name="Boston fern",
        watering_interval=WateringInterval(value=WEEK),
        light_requirement=LightRequirement.MEDIUM,
    )
    factory = await session_factory_on(database)
    async with factory() as session:
        await SqlAlchemySpeciesRepository(session, AggregateTracker()).add(species)
        await session.commit()
    return species


async def create_household(client: AsyncClient) -> str:
    """Create a household through the API and return its identifier."""
    response = await client.post("/api/v1/households", json={"name": "Home"})
    assert response.status_code == 201, response.text
    household_id: str = response.json()["household_id"]
    return household_id


async def create_plant(client: AsyncClient, household_id: str, species_id: str) -> str:
    """Create a plant through the API and return its identifier."""
    response = await client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": species_id,
            "name": "Fern",
            "location": "Shelf",
        },
    )
    assert response.status_code == 201, response.text
    plant_id: str = response.json()["plant_id"]
    return plant_id


async def wait_for_schedule(database: str, plant_id: str) -> datetime:
    """Poll until the onboarding saga has created the plant's schedule."""
    factory = await session_factory_on(database)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + SAGA_TIMEOUT_SECONDS
    while loop.time() < deadline:
        async with factory() as session:
            schedule = await SqlAlchemyCareScheduleRepository(session, AggregateTracker()).get(
                PlantId(uuid.UUID(plant_id))
            )
        if schedule is not None:
            return schedule.next_watering_at
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(f"no care schedule appeared for plant {plant_id} within {SAGA_TIMEOUT_SECONDS}s")


async def wait_for_outbox_event(database: str, event_name: str) -> OutboxModel:
    """Poll until an event of the given name is in the outbox."""
    factory = await session_factory_on(database)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + SAGA_TIMEOUT_SECONDS
    while loop.time() < deadline:
        async with factory() as session:
            statement = (
                select(OutboxModel)
                .where(OutboxModel.event_name == event_name)
                .order_by(OutboxModel.id)
            )
            row = (await session.execute(statement)).scalars().first()
        if row is not None:
            return row
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(f"no {event_name} event appeared in the outbox within {SAGA_TIMEOUT_SECONDS}s")


async def test_adding_a_plant_onboards_it(
    api_client: AsyncClient,
    database: str,
    worker_settings: Settings,
) -> None:
    species = await seed_species(database)
    household_id = await create_household(api_client)
    plant_id = await create_plant(api_client, household_id, str(species.id))

    async with running_worker_consumers(worker_settings):
        await publish_the_outbox(worker_settings)
        next_watering_at = await wait_for_schedule(database, plant_id)
        await wait_for_outbox_event(database, "PlantOnboarded")

    # The schedule starts one species interval out, so the plant is watered on the
    # catalogue's cadence rather than immediately.
    assert next_watering_at > datetime.now(UTC)
    assert next_watering_at < datetime.now(UTC) + 2 * WEEK

    factory = await session_factory_on(database)
    async with factory() as session:
        statement = select(NotificationModel).where(
            NotificationModel.household_id == uuid.UUID(household_id)
        )
        notifications = (await session.execute(statement)).scalars().all()

    assert [notification.notification_type for notification in notifications] == ["plant_onboarded"]
    assert notifications[0].payload["plant_id"] == plant_id
