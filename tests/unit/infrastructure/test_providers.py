"""Smoke tests for the dependency-injection wiring.

Neither test touches the network: creating an engine is lazy, and resolving a
handler only builds objects. That is enough to catch the failure mode these
tests exist for — a handler in the request map that the container cannot build,
or the other way round.
"""

from __future__ import annotations

from dishka import make_async_container

from plantkeeper.application.commands.garden import AddPlantHandler
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.repositories import PlantRepository
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.registry import build_request_map
from plantkeeper.infrastructure.di.providers import HANDLER_TYPES, api_providers


def test_every_bound_request_is_a_registered_handler() -> None:
    assert set(build_request_map().values()) == set(HANDLER_TYPES)


def test_every_registered_handler_is_bound_to_a_request() -> None:
    assert len(HANDLER_TYPES) == len(set(HANDLER_TYPES))
    assert set(build_request_map().values()) == set(HANDLER_TYPES)


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
