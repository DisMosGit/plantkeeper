"""Smoke tests for the dependency-injection wiring.

Neither test touches the network: creating an engine is lazy, and resolving a
handler only builds objects. That is enough to catch the failure mode these
tests exist for — a handler in the request map that the container cannot build,
or the other way round.
"""

from __future__ import annotations

from dishka import Provider, Scope, make_async_container, provide

from plantkeeper.application.commands.garden import AddPlantHandler
from plantkeeper.application.ports.catalog import SpeciesCache, SpeciesRecord, SpeciesSource
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.repositories import PlantRepository
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.queries.catalog import GetSpeciesQueryHandler
from plantkeeper.application.registry import build_request_map
from plantkeeper.application.sagas.registry import CONSUMER_TYPES, SAGA_TYPES, TRIGGER_TYPES
from plantkeeper.infrastructure.di.providers import (
    HANDLER_TYPES,
    SAGA_COMPONENT_TYPES,
    api_providers,
    worker_providers,
)


def test_every_bound_request_is_a_registered_handler() -> None:
    assert set(build_request_map().values()) == set(HANDLER_TYPES)


def test_every_registered_handler_is_bound_to_a_request() -> None:
    assert len(HANDLER_TYPES) == len(set(HANDLER_TYPES))
    assert set(build_request_map().values()) == set(HANDLER_TYPES)


def test_every_registered_consumer_is_buildable_by_the_worker_container() -> None:
    """A consumer the worker subscribes to but cannot build fails at delivery time."""
    registered = set(CONSUMER_TYPES) | set(TRIGGER_TYPES) | set(SAGA_TYPES)

    assert registered <= set(SAGA_COMPONENT_TYPES)


async def test_the_api_container_builds_a_handler_with_its_dependencies() -> None:
    container = make_async_container(*api_providers())
    try:
        async with container() as request_container:
            handler = await request_container.get(AddPlantHandler)
            unit_of_work = await request_container.get(UnitOfWork)
            plants = await request_container.get(PlantRepository)
            clock = await request_container.get(Clock)

        assert isinstance(handler, AddPlantHandler)
        assert isinstance(unit_of_work, UnitOfWork)
        assert isinstance(plants, PlantRepository)
        assert clock.now().tzinfo is not None
    finally:
        await container.close()


async def test_the_api_container_builds_the_cached_catalogue_query() -> None:
    """The catalogue query reads through Valkey, so the cache must be resolvable.

    Building the handler creates the Valkey client but opens no connection: the
    adapter dials only when a query runs.
    """
    container = make_async_container(*api_providers())
    try:
        async with container() as request_container:
            handler = await request_container.get(GetSpeciesQueryHandler)
            cache = await request_container.get(SpeciesCache)

        assert isinstance(handler, GetSpeciesQueryHandler)
        assert isinstance(cache, SpeciesCache)
    finally:
        await container.close()


class StubSpeciesSource:
    """A source that answers with nothing; only the binding is under test."""

    async def fetch_all(self) -> list[SpeciesRecord]:
        """Return no upstream entries."""
        return []


class StubSourceProvider(Provider):
    """Replaces the worker's Trefle source; the later provider wins in Dishka."""

    @provide(scope=Scope.APP)
    def species_source(self) -> SpeciesSource:
        """Return the stub instead of the Trefle adapter."""
        return StubSpeciesSource()


async def test_a_test_provider_can_replace_the_worker_upstream() -> None:
    """The catalogue e2e test swaps Trefle for an in-process transport this way."""
    container = make_async_container(*worker_providers(), StubSourceProvider())
    try:
        async with container() as request_container:
            source = await request_container.get(SpeciesSource)

        assert isinstance(source, StubSpeciesSource)
    finally:
        await container.close()
