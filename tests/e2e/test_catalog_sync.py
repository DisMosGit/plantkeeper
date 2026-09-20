"""End-to-end test of the catalogue synchronisation (Phase 9).

This is the phase's acceptance test: the manual ``POST /api/v1/catalog/sync``
writes ``SpeciesSyncRequested`` to the outbox, the relay publishes it, the
worker's real consumer registration runs ``SpeciesSyncSaga``, and the saga pulls
Trefle through the real anti-corruption layer — over an in-process transport, so
no test touches the network — and writes the result to ``write_catalog.species``.
A species that already existed locally is updated and its cached entry is dropped
by ``SpeciesCacheConsumer``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from aiolimiter import AsyncLimiter
from dishka import Provider, Scope, make_async_container, provide
from faststream.kafka import KafkaBroker
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from valkey.asyncio import Valkey

from plantkeeper.application.ports.catalog import SpeciesRecord, SpeciesSource
from plantkeeper.application.views import SpeciesView
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.domain.values import WateringInterval
from plantkeeper.infrastructure.cache.species import ValkeySpeciesCache, cache_key
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.providers import worker_providers
from plantkeeper.infrastructure.external.circuit_breaker import AsyncCircuitBreaker
from plantkeeper.infrastructure.external.trefle.client import TrefleClient
from plantkeeper.infrastructure.external.trefle.mapping import species_id_for
from plantkeeper.infrastructure.external.trefle.source import TrefleSpeciesSource
from plantkeeper.infrastructure.messaging.relay import OutboxRelay
from plantkeeper.infrastructure.persistence.models.catalog import SpeciesModel
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel
from plantkeeper.infrastructure.persistence.repositories.catalog import (
    SqlAlchemySpeciesRepository,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker
from plantkeeper.workers.consumers import register_consumers

pytestmark = pytest.mark.slow

SPECIES_COUNT = 30
PAGE_SIZE = 20
SYNC_TIMEOUT_SECONDS = 30.0
POLL_INTERVAL_SECONDS = 0.25
CACHED_SLUG = "catalog-sync-00"

SLUGS = [f"catalog-sync-{index:02d}" for index in range(SPECIES_COUNT)]


class FixedClock:
    """A clock for the breaker; nothing here depends on it moving."""

    def now(self) -> datetime:
        """Return a fixed instant."""
        return datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


class InMemorySnapshots:
    """The snapshot store the real source writes through, kept in memory."""

    def __init__(self) -> None:
        self.records: list[SpeciesRecord] | None = None

    async def load(self) -> list[SpeciesRecord] | None:
        """Return what was last saved."""
        return self.records

    async def save(self, records: Sequence[SpeciesRecord]) -> None:
        """Remember the last fetch."""
        self.records = list(records)


class FakeTrefleProvider(Provider):
    """Override the worker's upstream with the in-process Trefle source."""

    def __init__(self, source: SpeciesSource) -> None:
        super().__init__()
        self._source = source

    @provide(scope=Scope.APP)
    def species_source(self) -> SpeciesSource:
        """Return the Trefle source built over the mock transport."""
        return self._source


def a_list_item(slug: str) -> dict[str, object]:
    """One entry of a Trefle list page."""
    return {
        "id": SLUGS.index(slug) + 1,
        "slug": slug,
        "scientific_name": slug.replace("-", " ").title(),
        "common_name": slug,
    }


def catalogue_handler(request: httpx.Request) -> httpx.Response:
    """Serve two list pages and a detail per species, as Trefle does."""
    if request.url.path.endswith("/species"):
        page = int(request.url.params.get("page", "1"))
        start = (page - 1) * PAGE_SIZE
        items = [a_list_item(slug) for slug in SLUGS[start : start + PAGE_SIZE]]
        more = start + PAGE_SIZE < len(SLUGS)
        return httpx.Response(
            200,
            json={
                "data": items,
                "links": {"next": f"/api/v1/species?page={page + 1}" if more else None},
            },
        )
    slug = request.url.path.rsplit("/", 1)[-1]
    return httpx.Response(
        200,
        json={"data": {**a_list_item(slug), "growth": {"light": 7, "soil_humidity": 5}}},
    )


def a_trefle_source(client: httpx.AsyncClient) -> TrefleSpeciesSource:
    """The real source, over a transport that answers like Trefle."""
    return TrefleSpeciesSource(
        client=TrefleClient(
            client=client,
            limiter=AsyncLimiter(55, 60),
            breaker=AsyncCircuitBreaker(
                name="trefle",
                failure_threshold=5,
                reset_timeout=timedelta(seconds=60),
                clock=FixedClock(),
            ),
            token="test-token",
            species_limit=SPECIES_COUNT,
            max_attempts=3,
        ),
        snapshots=InMemorySnapshots(),
    )


async def session_factory_on(database: str) -> async_sessionmaker[AsyncSession]:
    """A session factory on the test database."""
    return async_sessionmaker(create_async_engine(database), expire_on_commit=False)


async def seed_species(database: str, species_id: SpeciesId) -> None:
    """Write one catalogue entry the synchronisation will then update."""
    species = Species.create(
        species_id=species_id,
        scientific_name="Placeholder",
        common_name="Placeholder",
        watering_interval=WateringInterval(value=timedelta(days=7)),
        light_requirement=LightRequirement.MEDIUM,
    )
    factory = await session_factory_on(database)
    async with factory() as session:
        await SqlAlchemySpeciesRepository(session, AggregateTracker()).add(species)
        await session.commit()


async def count_species(database: str) -> int:
    """How many catalogue rows exist."""
    factory = await session_factory_on(database)
    async with factory() as session:
        return len((await session.execute(select(SpeciesModel))).scalars().all())


async def read_species(database: str, species_id: SpeciesId) -> Species | None:
    """Read one catalogue entry back."""
    factory = await session_factory_on(database)
    async with factory() as session:
        return await SqlAlchemySpeciesRepository(session, AggregateTracker()).get(species_id)


async def outbox_event_names(database: str) -> list[str]:
    """The outbox in insertion order, as event names."""
    factory = await session_factory_on(database)
    async with factory() as session:
        statement = select(OutboxModel).order_by(OutboxModel.id)
        return [model.event_name for model in (await session.execute(statement)).scalars()]


async def wait_until(predicate: Callable[[], Awaitable[bool]], *, description: str) -> None:
    """Poll an async predicate until it is true, or fail the test."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + SYNC_TIMEOUT_SECONDS
    while loop.time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(f"timed out waiting for {description}")


@asynccontextmanager
async def running_worker(settings: Settings) -> AsyncIterator[None]:
    """Run the real worker: the relay plus every consumer, with the upstream replaced.

    The relay has to keep running (as ``make workers`` does): the saga's own
    follow-up events — ``SpeciesUpdated`` and ``SpeciesCacheInvalidated`` — are
    written to the outbox *while* the test waits, and only a running relay
    publishes them to the cache consumer.
    """
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(catalogue_handler), base_url="https://trefle.test/api/v1/"
    ) as trefle_client:
        container = make_async_container(
            *worker_providers(), FakeTrefleProvider(a_trefle_source(trefle_client))
        )
        broker = await container.get(KafkaBroker)
        relay = await container.get(OutboxRelay)
        register_consumers(broker, container=container, settings=settings)
        await broker.start()
        relay_task = asyncio.create_task(relay.run())
        try:
            yield
        finally:
            relay.stop()
            await relay_task
            await broker.stop()
            await container.close()


async def test_the_manual_sync_pulls_the_catalogue_and_drops_the_stale_cache_entry(
    api_client: AsyncClient,
    database: str,
    worker_settings: Settings,
    valkey_url: str,
) -> None:
    known_id = species_id_for(CACHED_SLUG)
    await seed_species(database, known_id)

    client = Valkey.from_url(valkey_url, decode_responses=True)
    cache = ValkeySpeciesCache(client, ttl_seconds=3_600)
    await cache.set(
        SpeciesView(
            species_id=known_id,
            scientific_name="Placeholder",
            common_name="Placeholder",
            watering_interval=timedelta(days=7),
            light_requirement=LightRequirement.MEDIUM,
            version=1,
        )
    )

    try:
        async with running_worker(worker_settings):
            response = await api_client.post("/api/v1/catalog/sync")
            assert response.status_code == 202, response.text

            async def all_species_are_local() -> bool:
                return await count_species(database) == SPECIES_COUNT

            async def the_update_landed() -> bool:
                updated = await read_species(database, known_id)
                return updated is not None and updated.common_name == CACHED_SLUG

            async def the_cache_entry_is_gone() -> bool:
                return await client.get(cache_key(known_id)) is None

            await wait_until(all_species_are_local, description=f"{SPECIES_COUNT} species")
            await wait_until(the_update_landed, description="the upstream update")
            await wait_until(the_cache_entry_is_gone, description="the cache invalidation")
    finally:
        await client.aclose()

    names = await outbox_event_names(database)
    assert names.count("SpeciesSyncRequested") == 1
    assert names.count("SpeciesUpdated") == 1
    assert names.count("SpeciesAdded") == SPECIES_COUNT - 1
    assert names.count("SpeciesCacheInvalidated") == 1
    assert names.count("SagaCompleted") == 1
