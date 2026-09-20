"""End-to-end test of the read path.

The write side's own end-to-end test stops at "the event is readable from Kafka".
This one covers the other half: the read-side process — the very Starlette
application ``make admin`` serves, with its real Kafka subscriptions — consumes
that event, projects it into ``read_analytics``, and Django Admin shows it.

The admin application is started inside the test rather than left running in the
background: the same code path, without a loop whose failures would look like
flakes. Each test uses consumer groups of its own, so a previous test's offsets
cannot hide the events this one projects.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

import pytest
from aiokafka import AIOKafkaProducer
from dishka import make_async_container
from faststream.kafka import KafkaBroker
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.applications import Starlette

from plantkeeper.admin.asgi import create_admin_application
from plantkeeper.admin.read_models.models import PlantReadModel, ProcessedEvent
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.messaging.relay import OutboxRelay
from plantkeeper.infrastructure.messaging.topics import (
    HEADER_EVENT_ID,
    HEADER_EVENT_NAME,
)
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel

pytestmark = pytest.mark.slow

PROJECTION_TIMEOUT_SECONDS = 30.0
"""How long the consumer is given to project one event."""

DUPLICATE_SETTLE_SECONDS = 2.0
"""How long a redelivered duplicate is given to be seen and ignored."""


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


async def outbox_rows(database: str) -> list[OutboxModel]:
    """Return every outbox row, oldest first."""
    engine = create_async_engine(database)
    try:
        factory = async_sessionmaker(engine)
        async with factory() as session:
            statement = select(OutboxModel).order_by(OutboxModel.id)
            return list((await session.execute(statement)).scalars().all())
    finally:
        await engine.dispose()


async def republish(row: OutboxModel, *, bootstrap_servers: str) -> None:
    """Put an identical message back on its topic, as an at-least-once broker would.

    The body is the outbox payload and the headers are the two the relay wrote, so
    the consumer sees a delivery it cannot distinguish from the first one. That is
    the case ``ProcessedEvent`` exists for.
    """
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        await producer.send_and_wait(
            row.topic,
            value=json.dumps(row.payload, separators=(",", ":")).encode("utf-8"),
            key=row.partition_key.encode("utf-8"),
            headers=[
                (HEADER_EVENT_NAME, row.event_name.encode("utf-8")),
                (HEADER_EVENT_ID, str(row.event_id).encode("utf-8")),
            ],
        )
    finally:
        await producer.stop()


@asynccontextmanager
async def running_admin(settings: Settings) -> AsyncIterator[Starlette]:
    """Run the read-side application, Kafka subscriptions and all."""
    application = create_admin_application(settings=settings)
    async with application.router.lifespan_context(application):
        yield application


async def create_household(client: AsyncClient, name: str = "Home") -> str:
    """Create a household through the API and return its identifier."""
    response = await client.post("/api/v1/households", json={"name": name})
    assert response.status_code == 201, response.text
    household_id: str = response.json()["household_id"]
    return household_id


async def create_plant(client: AsyncClient, household_id: str, *, name: str = "Fern") -> str:
    """Create a plant through the API and return its identifier."""
    response = await client.post(
        "/api/v1/plants",
        json={
            "household_id": household_id,
            "species_id": str(uuid.uuid4()),
            "name": name,
            "location": "Shelf",
        },
    )
    assert response.status_code == 201, response.text
    plant_id: str = response.json()["plant_id"]
    return plant_id


async def wait_for_plant(plant_id: str) -> PlantReadModel:
    """Wait until the projection has written the plant, or fail.

    The projection is asynchronous by nature: the event is published by the relay
    and consumed whenever the broker delivers it, so a timeout is the only honest
    way to wait for it.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + PROJECTION_TIMEOUT_SECONDS
    while loop.time() < deadline:
        row = await PlantReadModel.objects.filter(plant_id=uuid.UUID(plant_id)).afirst()
        if row is not None:
            return cast("PlantReadModel", row)
        await asyncio.sleep(0.25)
    pytest.fail(f"plant {plant_id} was not projected within {PROJECTION_TIMEOUT_SECONDS}s")


async def test_a_created_plant_is_projected_into_the_read_model(
    api_client: AsyncClient,
    database: str,
    read_side_settings: Settings,
    read_side_database: str,
) -> None:
    household_id = await create_household(api_client)

    async with running_admin(read_side_settings):
        plant_id = await create_plant(api_client, household_id)
        await publish_the_outbox(read_side_settings)

        row = await wait_for_plant(plant_id)

    assert row.household_id == uuid.UUID(household_id)
    assert row.name == "Fern"
    assert row.location == "Shelf"
    assert row.removed is False
    assert row.added_at is not None
    # Care has not run for this plant: the API creates no schedule (Phase 4 does).
    assert row.next_watering_at is None

    # The event that produced the row is in the ledger, under the garden group.
    rows = await outbox_rows(database)
    added = next(row for row in rows if row.event_name == "PlantAdded")
    assert (
        await ProcessedEvent.objects.filter(
            consumer_group=f"{read_side_settings.read_side_consumer_group_prefix}-garden",
            event_id=added.event_id,
        ).acount()
        == 1
    )


async def test_a_redelivered_event_is_projected_once(
    api_client: AsyncClient,
    database: str,
    kafka_bootstrap_servers: str,
    read_side_settings: Settings,
    read_side_database: str,
) -> None:
    """At-least-once delivery is the contract; projecting once is the answer."""
    household_id = await create_household(api_client)

    async with running_admin(read_side_settings):
        plant_id = await create_plant(api_client, household_id)
        await publish_the_outbox(read_side_settings)
        row = await wait_for_plant(plant_id)
        assert row.name == "Fern"

        pending = await outbox_rows(database)
        added = next(row for row in pending if row.event_name == "PlantAdded")

        await republish(added, bootstrap_servers=kafka_bootstrap_servers)
        # The duplicate is a no-op in the ledger, so there is nothing to poll for:
        # the delivery is given time to arrive, and the counts must not move.
        await asyncio.sleep(DUPLICATE_SETTLE_SECONDS)

    assert await PlantReadModel.objects.filter(plant_id=uuid.UUID(plant_id)).acount() == 1
    assert await ProcessedEvent.objects.filter(event_id=added.event_id).acount() == 1


async def test_django_admin_shows_the_projected_plant_without_a_login_form(
    api_client: AsyncClient,
    read_side_settings: Settings,
    read_side_database: str,
) -> None:
    household_id = await create_household(api_client)

    async with running_admin(read_side_settings) as admin:
        plant_id = await create_plant(api_client, household_id, name="Monstera")
        await publish_the_outbox(read_side_settings)
        await wait_for_plant(plant_id)

        transport = ASGITransport(app=admin)
        async with AsyncClient(transport=transport, base_url="http://testserver") as browser:
            login = await browser.get("/admin/login/")

            index = await browser.get("/admin/")
            changelist = await browser.get("/admin/read_models/plantreadmodel/")
            schedule = await browser.get("/admin/read_models/carereadmodel/")
            journal = await browser.get("/admin/read_models/journalreadmodel/")

    # The middleware selected the local user before the view ran, so the login
    # form is never served: Django redirects an authenticated staff user away.
    assert login.status_code == 302, login.text
    assert login.headers["location"] == "/admin/"

    assert index.status_code == 200, index.text
    assert "Projections" in index.text

    assert changelist.status_code == 200, changelist.text
    assert "Monstera" in changelist.text

    # The other projections read well even when nothing has fed them yet.
    assert schedule.status_code == 200, schedule.text
    assert journal.status_code == 200, journal.text


async def test_the_journal_inline_renders_on_the_plant_page(
    api_client: AsyncClient,
    read_side_settings: Settings,
    read_side_database: str,
) -> None:
    """The plant page carries the journal the roadmap asks for."""
    household_id = await create_household(api_client)

    async with running_admin(read_side_settings) as admin:
        plant_id = await create_plant(api_client, household_id, name="Fern")
        await publish_the_outbox(read_side_settings)
        await wait_for_plant(plant_id)

        transport = ASGITransport(app=admin)
        async with AsyncClient(transport=transport, base_url="http://testserver") as browser:
            detail: Response = await browser.get(
                f"/admin/read_models/plantreadmodel/{plant_id}/change/"
            )

    assert detail.status_code == 200, detail.text
    # Nothing publishes JournalEntryAdded before Phase 6, so the inline is present
    # with its headings and no rows — which is exactly what the phase delivers.
    assert "Journal entries" in detail.text


def test_the_read_model_matches_the_write_side_surface() -> None:
    """A smoke assertion that the read side did not drift into another schema name."""
    assert PlantReadModel._meta.db_table == '"read_analytics"."plants"'
