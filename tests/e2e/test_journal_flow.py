"""End-to-end test of the event-sourced journal.

Phase 6's acceptance test: a watering goes through the real API, the relay publishes
``WateringCompleted`` to Kafka, the worker's real consumer registration feeds it to
``JournalEntryConsumer``, and the plant's journal — replayed from the event store —
carries the entry. The dated endpoint is then asked the same question the roadmap
asks: what did the journal look like before that day, and on it?

The pieces the API and the worker share are the production ones, so this exercises
the wiring rather than a test-only path.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
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

PIPELINE_TIMEOUT_SECONDS = 45.0
POLL_INTERVAL_SECONDS = 0.25
WEEK = timedelta(days=7)


def session_factory_on(database: str) -> async_sessionmaker[AsyncSession]:
    """A session factory on the test database."""
    return async_sessionmaker(create_async_engine(database), expire_on_commit=False)


async def poll_until[PolledT](
    check: Callable[[], Awaitable[PolledT | None]],
    *,
    description: str,
    timeout: float = PIPELINE_TIMEOUT_SECONDS,
) -> PolledT:
    """Poll ``check`` until it answers something, or fail the test.

    Polling rather than sleeping a fixed span: the pipeline crosses Kafka and a
    database transaction, and a fixed wait would either be a flake or waste the
    suite's time.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        found = await check()
        if found is not None:
            return found
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(f"{description} did not happen within {timeout}s")


@asynccontextmanager
async def running_worker_consumers(settings: Settings) -> AsyncIterator[None]:
    """Run the worker's consumers in-process, as ``make workers`` would.

    The registration call is the production one — the same call ``apps/workers``
    makes before it starts the broker — so this exercises the wiring.
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
    factory = session_factory_on(database)
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


async def wait_for_schedule(database: str, plant_id: str) -> None:
    """Poll until the onboarding saga has created the plant's schedule."""
    factory = session_factory_on(database)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + PIPELINE_TIMEOUT_SECONDS
    while loop.time() < deadline:
        async with factory() as session:
            schedule = await SqlAlchemyCareScheduleRepository(session, AggregateTracker()).get(
                PlantId(uuid.UUID(plant_id))
            )
        if schedule is not None:
            return
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(f"no care schedule appeared for plant {plant_id}")


async def watering_completed_at(database: str) -> datetime:
    """Return the care moment of the ``WateringCompleted`` the API recorded."""
    factory = session_factory_on(database)
    async with factory() as session:
        statement = (
            select(OutboxModel)
            .where(OutboxModel.event_name == "WateringCompleted")
            .order_by(OutboxModel.id)
        )
        row = (await session.execute(statement)).scalars().first()
    assert row is not None, "the watering should have left a WateringCompleted in the outbox"
    return datetime.fromisoformat(str(row.payload["completed_at"]))


async def a_watered_plant(
    api_client: AsyncClient, database: str, settings: Settings
) -> tuple[str, datetime]:
    """Onboard one plant and water it, returning its id and the care moment."""
    species = await seed_species(database)
    household_id = await create_household(api_client)
    plant_id = await create_plant(api_client, household_id, str(species.id))

    async with running_worker_consumers(settings):
        await publish_the_outbox(settings)
        await wait_for_schedule(database, plant_id)

        watered = await api_client.post(f"/api/v1/care/{plant_id}/water")
        assert watered.status_code == 200, watered.text
        care_moment = await watering_completed_at(database)

        await publish_the_outbox(settings)

        async def journalled() -> list[dict[str, object]] | None:
            response = await api_client.get(f"/api/v1/journal/{plant_id}")
            assert response.status_code == 200, response.text
            items: list[dict[str, object]] = response.json()["items"]
            return items or None

        await poll_until(journalled, description="the watering reaching the journal")

    return plant_id, care_moment


async def test_a_watering_through_http_lands_in_the_event_sourced_journal(
    api_client: AsyncClient,
    database: str,
    worker_settings: Settings,
) -> None:
    plant_id, care_moment = await a_watered_plant(api_client, database, worker_settings)

    response = await api_client.get(f"/api/v1/journal/{plant_id}")
    assert response.status_code == 200, response.text
    items = response.json()["items"]

    assert len(items) == 1
    entry = items[0]
    assert entry["plant_id"] == plant_id
    assert entry["entry_type"] == "watering"
    assert datetime.fromisoformat(entry["occurred_at"]) == care_moment
    assert entry["note"] is None


async def test_the_journal_can_be_replayed_to_a_past_date(
    api_client: AsyncClient,
    database: str,
    worker_settings: Settings,
) -> None:
    plant_id, care_moment = await a_watered_plant(api_client, database, worker_settings)
    cared_on = care_moment.date()

    before = await api_client.get(
        f"/api/v1/journal/{plant_id}/at",
        params={"date": (cared_on - timedelta(days=1)).isoformat()},
    )
    assert before.status_code == 200, before.text
    assert before.json()["entry_count"] == 0
    assert before.json()["entries"] == []

    on_the_day = await api_client.get(
        f"/api/v1/journal/{plant_id}/at", params={"date": cared_on.isoformat()}
    )
    assert on_the_day.status_code == 200, on_the_day.text
    state = on_the_day.json()
    assert state["entry_count"] == 1
    assert state["counts_by_type"] == {"watering": 1}
    assert state["entries"][0]["entry_type"] == "watering"


async def test_the_journal_of_an_unknown_plant_is_a_404(
    api_client: AsyncClient,
) -> None:
    missing = str(uuid.uuid4())

    timeline = await api_client.get(f"/api/v1/journal/{missing}")
    as_of = await api_client.get(
        f"/api/v1/journal/{missing}/at", params={"date": datetime.now(UTC).date().isoformat()}
    )

    assert timeline.status_code == 404
    assert as_of.status_code == 404
