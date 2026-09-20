"""Unit tests for the catalogue's read path and its cache invalidator.

The query handler and the consumer both depend on ports, so both are exercised
with doubles: the repository and the cache for the handler, and just the cache for
the consumer, which only has to turn two event types into one ``invalidate``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import cast

import pytest
from cqrs.dispatcher.saga import SagaDispatcher

from plantkeeper.application.catalog.consumer import SpeciesCacheConsumer
from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.ports.catalog import SpeciesCache
from plantkeeper.application.ports.repositories import SpeciesRepository
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.queries.catalog import GetSpeciesQuery, GetSpeciesQueryHandler
from plantkeeper.application.views import SpeciesView
from plantkeeper.domain.catalog.events import (
    SpeciesCacheInvalidated,
    SpeciesSyncRequested,
    SpeciesUpdated,
)
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.domain.values import WateringInterval

INTERVAL = WateringInterval(value=timedelta(days=7))
SPECIES = SpeciesId.new()


def a_species(*, common_name: str = "Boston fern") -> Species:
    """One local catalogue entry."""
    return Species.create(
        species_id=SPECIES,
        scientific_name="Nephrolepis exaltata",
        common_name=common_name,
        watering_interval=INTERVAL,
        light_requirement=LightRequirement.MEDIUM,
    )


def a_view(*, common_name: str = "Boston fern") -> SpeciesView:
    """The view of :func:`a_species`."""
    return SpeciesView.from_domain(a_species(common_name=common_name))


class StubSpeciesRepository:
    """Answers one species, and counts the reads."""

    def __init__(self, species: Species | None) -> None:
        self.species = species
        self.reads = 0

    async def get(self, species_id: SpeciesId) -> Species | None:
        """Return the configured species."""
        self.reads += 1
        return self.species


class StubCache:
    """A cache that returns one value and remembers everything written to it."""

    def __init__(self, cached: SpeciesView | None = None) -> None:
        self.cached = cached
        self.written: list[SpeciesView] = []
        self.invalidated: list[SpeciesId] = []

    async def get(self, species_id: SpeciesId) -> SpeciesView | None:
        """Return whatever is cached right now."""
        return self.cached

    async def set(self, species: SpeciesView) -> None:
        """Remember the write and serve it from then on."""
        self.written.append(species)
        self.cached = species

    async def invalidate(self, species_ids: Sequence[SpeciesId]) -> None:
        """Forget the entries the caller declares stale."""
        self.invalidated.extend(species_ids)
        self.cached = None


def a_handler(repository: StubSpeciesRepository, cache: StubCache) -> GetSpeciesQueryHandler:
    """Build the query handler over the doubles."""
    return GetSpeciesQueryHandler(
        cast("SpeciesRepository", repository), cast("SpeciesCache", cache)
    )


async def test_a_cached_species_is_answered_without_touching_the_repository() -> None:
    repository = StubSpeciesRepository(a_species(common_name="Sword fern"))
    cache = StubCache(a_view())

    view = await a_handler(repository, cache).handle(GetSpeciesQuery(species_id=SPECIES))

    assert view == a_view()
    assert repository.reads == 0


async def test_a_miss_reads_the_repository_and_fills_the_cache() -> None:
    repository = StubSpeciesRepository(a_species())
    cache = StubCache()

    view = await a_handler(repository, cache).handle(GetSpeciesQuery(species_id=SPECIES))

    assert view == a_view()
    assert repository.reads == 1
    assert cache.written == [a_view()]


async def test_a_species_nobody_knows_is_not_found() -> None:
    repository = StubSpeciesRepository(None)

    with pytest.raises(NotFoundError, match="does not exist"):
        await a_handler(repository, StubCache()).handle(GetSpeciesQuery(species_id=SPECIES))


def a_consumer(cache: StubCache) -> SpeciesCacheConsumer:
    """Build the consumer over the cache double; no database is touched."""
    return SpeciesCacheConsumer(cast("UnitOfWork", None), cast("SpeciesCache", cache))


async def test_a_species_update_invalidates_that_species() -> None:
    cache = StubCache()

    await a_consumer(cache).handle(
        SpeciesUpdated(
            species_id=SPECIES,
            scientific_name="Nephrolepis exaltata",
            common_name="Sword fern",
            watering_interval=INTERVAL,
            light_requirement=LightRequirement.HIGH,
            version=2,
        ),
        cast("SagaDispatcher", None),
    )

    assert cache.invalidated == [SPECIES]


async def test_an_invalidation_event_invalidates_that_species() -> None:
    cache = StubCache()

    await a_consumer(cache).handle(
        SpeciesCacheInvalidated(species_id=SPECIES), cast("SagaDispatcher", None)
    )

    assert cache.invalidated == [SPECIES]


def test_the_consumer_claims_exactly_the_two_invalidation_events() -> None:
    consumer = a_consumer(StubCache())
    updated = SpeciesUpdated(
        species_id=SPECIES,
        scientific_name="Nephrolepis exaltata",
        common_name="Sword fern",
        watering_interval=INTERVAL,
        light_requirement=LightRequirement.MEDIUM,
        version=2,
    )

    assert consumer.handles(updated)
    assert consumer.handles(SpeciesCacheInvalidated(species_id=SPECIES))
    assert not consumer.handles(SpeciesSyncRequested())
