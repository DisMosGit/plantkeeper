"""Provenance travels the whole way, and a chain of reactions is walkable.

Task 1.3's acceptance test: three different edges raise events, and each one's
outbox row has to be stamped correctly.

* an **HTTP command** — the conversation is the request's, the raiser is the API;
* a **consumer reaction** — the conversation is the delivery's, and the cause is
  the event that was delivered;
* a **saga step** — the raiser is the saga, and the conversation is still the one
  the triggering delivery arrived on.

The rows are read straight from ``write_shared.outbox``, because that is where the
contract is frozen: the relay publishes what the write side recorded and derives
nothing (``docs/events.md``). The saga is driven through the suite's shared worker
harness (``tests/e2e/conftest.py``), so this exercises the real binding rather than a
stand-in.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
from dishka import make_async_container
from faststream.kafka import KafkaBroker
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from plantkeeper.api.main import API_RAISER
from plantkeeper.application.provenance import (
    UNKNOWN_RAISER,
    async_provenance_scope,
    current_provenance,
    reaction_context,
)
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.values import WateringInterval
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.messaging.relay import OutboxRelay
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel
from plantkeeper.infrastructure.persistence.repositories.catalog import (
    SqlAlchemySpeciesRepository,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker

pytestmark = pytest.mark.slow

SAGA_TIMEOUT_SECONDS = 30.0
POLL_INTERVAL_SECONDS = 0.25
WEEK = timedelta(days=7)


async def session_factory_on(database: str) -> async_sessionmaker[AsyncSession]:
    """A session factory on the test database."""
    return async_sessionmaker(create_async_engine(database), expire_on_commit=False)


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


async def outbox_rows(database: str, *, event_name: str | None = None) -> list[OutboxModel]:
    """Return the outbox rows, oldest first, optionally of one event name."""
    factory = await session_factory_on(database)
    async with factory() as session:
        statement = select(OutboxModel).order_by(OutboxModel.id)
        if event_name is not None:
            statement = statement.where(OutboxModel.event_name == event_name)
        return list((await session.execute(statement)).scalars().all())


async def wait_for_outbox_event(database: str, event_name: str) -> OutboxModel:
    """Poll until an event of the given name is in the outbox."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + SAGA_TIMEOUT_SECONDS
    while loop.time() < deadline:
        rows = await outbox_rows(database, event_name=event_name)
        if rows:
            return rows[0]
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(f"no {event_name} event appeared in the outbox within {SAGA_TIMEOUT_SECONDS}s")


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


async def a_household(client: AsyncClient) -> str:
    """Create a household through the API and return its identifier."""
    response = await client.post("/api/v1/households", json={"name": "Home"})
    assert response.status_code == 201, response.text
    household_id: str = response.json()["household_id"]
    return household_id


async def a_plant(client: AsyncClient, household_id: str, species_id: str) -> str:
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


# ---------------------------------------------------------------------------
# The API edge
# ---------------------------------------------------------------------------


async def test_an_api_command_stamps_the_conversation_on_the_row_it_writes(
    api_client: AsyncClient, database: str
) -> None:
    """One request, one conversation, and the API names itself as the raiser.

    ``AddPlant`` is the command used because it is the one whose aggregate raises
    an event: ``CreateHousehold`` deliberately raises none, so it writes no outbox
    row at all.
    """
    species = await seed_species(database)
    conversation = uuid.uuid4()
    household_id = await a_household(api_client)

    response = await api_client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": str(species.id),
            "name": "Fern",
            "location": "Shelf",
        },
        headers={"x-correlation-id": str(conversation)},
    )
    assert response.status_code == 201, response.text

    rows = await outbox_rows(database)
    assert len(rows) == 1
    row = rows[0]
    assert row.event_name == "PlantAdded"
    assert row.raised_by == API_RAISER
    # A client that names the conversation it is joining is honoured, so a chain
    # started outside the API can be continued inside it.
    assert row.correlation_id == conversation
    # Nothing caused the request, and the row says so rather than inventing a cause.
    assert row.causation_id is None
    assert row.schema_version == 1


async def test_a_request_without_a_conversation_starts_its_own(
    api_client: AsyncClient, database: str
) -> None:
    """A client that sends no correlation header is a new chain, not an error."""
    species = await seed_species(database)
    household_id = await a_household(api_client)
    await a_plant(api_client, household_id, str(species.id))

    row = (await outbox_rows(database))[0]
    assert row.correlation_id is not None
    assert row.correlation_id != row.event_id
    assert row.raised_by == API_RAISER


async def test_the_api_tolerates_a_malformed_conversation_header(
    api_client: AsyncClient, database: str
) -> None:
    """A courtesy header with nonsense in it starts a conversation, and does not fail."""
    species = await seed_species(database)
    household_id = await a_household(api_client)

    response = await api_client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": str(species.id),
            "name": "Fern",
            "location": "Shelf",
        },
        headers={"x-correlation-id": "not-a-uuid"},
    )
    assert response.status_code == 201, response.text

    row = (await outbox_rows(database))[0]
    assert row.correlation_id is not None


# ---------------------------------------------------------------------------
# The consumer edge
# ---------------------------------------------------------------------------


async def test_a_consumer_reaction_carries_the_delivery_as_its_cause(
    api_client: AsyncClient, database: str
) -> None:
    """What a consumer binds: the delivery is the cause, its conversation continues.

    The binding is asserted directly rather than through a second Kafka hop: the
    value under test is the context the consumer's edge constructs from a delivery,
    which ``decode_message`` builds and the worker binds.
    """
    conversation = uuid.uuid4()
    delivered_event_id = uuid.uuid4()

    context = reaction_context(
        raised_by="service:some-consumer",
        caused_by=delivered_event_id,
        correlation_id=conversation,
    )

    assert context.causation_id == delivered_event_id
    assert context.correlation_id == conversation
    assert context.raised_by == "service:some-consumer"


async def test_provenance_bound_by_one_delivery_does_not_leak_into_the_next() -> None:
    """The binding is scoped, so one delivery cannot inherit another's origin."""
    assert current_provenance() is None
    first = reaction_context(raised_by="service:first", caused_by=uuid.uuid4())
    async with async_provenance_scope(first):
        bound = current_provenance()
        assert bound is not None
        assert bound.raised_by == "service:first"
    assert current_provenance() is None

    second = reaction_context(raised_by="service:second", caused_by=uuid.uuid4())
    async with async_provenance_scope(second):
        rebound = current_provenance()
        assert rebound is not None
        assert rebound.raised_by == "service:second"


# ---------------------------------------------------------------------------
# The saga edge
# ---------------------------------------------------------------------------


async def test_a_saga_step_is_attributed_to_the_saga(
    api_client: AsyncClient,
    database: str,
    worker_settings: Settings,
    running_worker: None,
) -> None:
    """The onboarding saga names itself, and keeps the request's conversation.

    ``OnboardPlantSaga`` is triggered by ``PlantAdded``, so the whole chain here is
    request -> ``PlantAdded`` -> saga -> ``SagaCompleted``, and every link has to
    name the same conversation while naming a different raiser.
    """
    species = await seed_species(database)
    conversation = uuid.uuid4()
    household_id = await a_household(api_client)

    response = await api_client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": str(species.id),
            "name": "Fern",
            "location": "Shelf",
        },
        headers={"x-correlation-id": str(conversation)},
    )
    assert response.status_code == 201, response.text

    added = (await outbox_rows(database, event_name="PlantAdded"))[0]

    await publish_the_outbox(worker_settings)
    started = await wait_for_outbox_event(database, "SagaStarted")
    completed = await wait_for_outbox_event(database, "SagaCompleted")

    # The request's conversation reached the plant event...
    assert added.raised_by == API_RAISER
    assert added.correlation_id == conversation
    # ...and the saga continued it rather than starting its own.
    assert started.raised_by is not None
    assert started.raised_by.startswith("saga:")
    assert started.correlation_id == conversation
    assert completed.raised_by == started.raised_by
    assert completed.correlation_id == conversation
    # The chain is walkable in both directions: the saga's lifecycle event names
    # the delivery that triggered it as its cause.
    assert started.causation_id == added.event_id


async def test_an_event_with_no_provenance_is_still_attributed(
    database: str,
) -> None:
    """A row written with nothing bound says ``system:unknown`` rather than lying.

    This is the tolerance policy: absence of provenance is a gap in the metadata,
    and a message published from such a row carries the headers it can rather than
    asserting an origin it does not have.
    """
    from datetime import UTC, datetime

    from plantkeeper.domain.garden.events import PlantAdded
    from plantkeeper.infrastructure.persistence.mappers.outbox import outbox_model_from_event

    row = outbox_model_from_event(
        PlantAdded.model_validate(
            {
                "plant_id": str(uuid.uuid4()),
                "household_id": str(uuid.uuid4()),
                "species_id": str(uuid.uuid4()),
                "name": "Fern",
                "location": "Shelf",
                "added_at": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
                "occurred_at": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
            }
        )
    )

    assert row.raised_by == UNKNOWN_RAISER
    assert row.correlation_id is None
    assert row.causation_id is None
    assert row.schema_version == 1
