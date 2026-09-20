"""The catalogue cache and its invalidator, against real Valkey and Postgres.

The unit suite pins the cache's behaviour over a double; this file pins the parts
a double cannot: the JSON that actually goes on the wire, the TTL the server
applies, and the ``(consumer_group, event_id)`` claim that makes a redelivered
invalidation a no-op.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.requests.map import SagaMap
from cqrs.saga.storage.protocol import ISagaStorage
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from valkey.asyncio import Valkey

from plantkeeper.application.catalog.consumer import SpeciesCacheConsumer
from plantkeeper.application.views import SpeciesView
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.catalog.events import SpeciesCacheInvalidated, SpeciesUpdated
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.domain.values import WateringInterval
from plantkeeper.infrastructure.cache.species import ValkeySpeciesCache, cache_key
from plantkeeper.infrastructure.persistence.saga_storage import SqlAlchemySagaStorage
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork

pytestmark = pytest.mark.integration

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
TTL_SECONDS = 3_600
GROUP = "test-species-cache"
SPECIES = SpeciesId.new()


class EmptyContainer:
    """A cqrs container for a consumer that never dispatches a saga."""

    async def resolve[ResolvedT](self, type_: type[ResolvedT]) -> ResolvedT:
        """Never called by this consumer."""
        raise KeyError(type_)


def a_dispatcher(storage: ISagaStorage) -> SagaDispatcher:
    """A dispatcher the cache consumer accepts and ignores."""
    return SagaDispatcher(SagaMap(), EmptyContainer(), storage)


def a_view(*, common_name: str = "Boston fern") -> SpeciesView:
    """One catalogue view."""
    return SpeciesView(
        species_id=SPECIES,
        scientific_name="Nephrolepis exaltata",
        common_name=common_name,
        watering_interval=timedelta(days=7),
        light_requirement=LightRequirement.MEDIUM,
        version=2,
    )


def a_species_updated() -> SpeciesUpdated:
    """One catalogue change."""
    return SpeciesUpdated(
        species_id=SPECIES,
        scientific_name="Nephrolepis exaltata",
        common_name="Sword fern",
        watering_interval=WateringInterval(value=timedelta(days=3)),
        light_requirement=LightRequirement.HIGH,
        version=3,
    )


async def run_consumer(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    event: DomainEvent,
    cache: ValkeySpeciesCache,
) -> bool:
    """Deliver one event to the cache consumer, exactly as the worker does."""
    storage = SqlAlchemySagaStorage(session_factory)
    async with session_factory() as session:
        unit_of_work = SqlAlchemyUnitOfWork(session)
        consumer = SpeciesCacheConsumer(unit_of_work, cache)
        return await consumer.consume(event, consumer_group=GROUP, dispatcher=a_dispatcher(storage))


@pytest.fixture
async def valkey(valkey_url: str) -> AsyncIterator[Valkey]:
    """A client on the session's Valkey container."""
    client = Valkey.from_url(valkey_url, decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def cache(valkey: Valkey) -> ValkeySpeciesCache:
    return ValkeySpeciesCache(valkey, ttl_seconds=TTL_SECONDS)


async def test_a_view_round_trips_through_valkey(valkey: Valkey, cache: ValkeySpeciesCache) -> None:
    await valkey.delete(cache_key(SPECIES))

    await cache.set(a_view())

    assert await cache.get(SPECIES) == a_view()


async def test_the_entry_gets_the_configured_ttl(valkey: Valkey, cache: ValkeySpeciesCache) -> None:
    await cache.set(a_view())

    ttl = await valkey.ttl(cache_key(SPECIES))
    assert 0 < ttl <= TTL_SECONDS


async def test_invalidate_drops_the_entry(valkey: Valkey, cache: ValkeySpeciesCache) -> None:
    await cache.set(a_view())

    await cache.invalidate([SPECIES])

    assert await valkey.get(cache_key(SPECIES)) is None
    assert await cache.get(SPECIES) is None


async def test_an_unparsable_entry_is_dropped(valkey: Valkey, cache: ValkeySpeciesCache) -> None:
    await valkey.set(cache_key(SPECIES), "not json")

    assert await cache.get(SPECIES) is None
    assert await valkey.get(cache_key(SPECIES)) is None


async def test_a_species_update_drops_the_cached_entry(
    session_factory: async_sessionmaker[AsyncSession],
    valkey: Valkey,
    cache: ValkeySpeciesCache,
) -> None:
    await cache.set(a_view())

    assert await run_consumer(session_factory, event=a_species_updated(), cache=cache) is True

    assert await valkey.get(cache_key(SPECIES)) is None


async def test_an_explicit_invalidation_drops_the_cached_entry(
    session_factory: async_sessionmaker[AsyncSession],
    valkey: Valkey,
    cache: ValkeySpeciesCache,
) -> None:
    await cache.set(a_view())

    assert (
        await run_consumer(
            session_factory, event=SpeciesCacheInvalidated(species_id=SPECIES), cache=cache
        )
        is True
    )

    assert await valkey.get(cache_key(SPECIES)) is None


async def test_a_redelivered_invalidation_is_a_no_op(
    session_factory: async_sessionmaker[AsyncSession],
    cache: ValkeySpeciesCache,
) -> None:
    """The ledger claim comes first: a second delivery must not do the work twice."""
    event = SpeciesCacheInvalidated(species_id=SPECIES)
    assert await run_consumer(session_factory, event=event, cache=cache) is True

    # Repopulate the cache, then redeliver the same event in the same group.
    await cache.set(a_view())
    assert await run_consumer(session_factory, event=event, cache=cache) is False

    assert await cache.get(SPECIES) is not None
