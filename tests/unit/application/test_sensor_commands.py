"""Registering a sensor, with an identifier the caller already owns.

The write side normally mints a sensor's identity, and this test keeps that as the
default while pinning the exception: when a caller owns the identifier — the IoT
simulator derives its sensors from a base id it prints — the command must register
*that* sensor, or the telemetry it publishes will never have a plant.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import TracebackType
from typing import Self, cast
from uuid import uuid4

import pytest

from plantkeeper.application.commands.telemetry import AddSensorCommand, AddSensorHandler
from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SensorId, SpeciesId
from plantkeeper.domain.telemetry.sensor import Sensor
from plantkeeper.domain.values import Location

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)


class FakeClock:
    """A clock frozen at the instant the test works from."""

    def now(self) -> datetime:
        """Return the fixed instant."""
        return NOW


class FakePlants:
    """A plant repository holding whatever the test seeded."""

    def __init__(self, plant: Plant | None) -> None:
        self.plant = plant

    async def get(self, plant_id: PlantId) -> Plant | None:
        """Return the seeded plant when the id matches."""
        return self.plant if self.plant is not None and self.plant.id == plant_id else None


class FakeSensors:
    """A sensor repository that records what was added."""

    def __init__(self) -> None:
        self.added: list[Sensor] = []

    async def add(self, sensor: Sensor) -> None:
        """Record one registration."""
        self.added.append(sensor)


class FakeIdempotency:
    """Enough of the idempotency store for a command with no key."""

    async def get(self, key: str) -> None:
        """Report every key as unknown."""
        return None


class FakeOutbox:
    """A sink for whatever the unit of work drains."""

    async def append(self, event: object) -> None:
        """Accept and forget an event."""
        return None


class FakeUnitOfWork:
    """One transaction over the sensor and plant fakes."""

    def __init__(self, plant: Plant | None, sensors: FakeSensors) -> None:
        self.plants = FakePlants(plant)
        self.sensors = sensors
        self.idempotency = FakeIdempotency()
        self.outbox = FakeOutbox()
        self.commits = 0

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    async def commit(self) -> None:
        """Count the commit."""
        self.commits += 1


def a_plant() -> Plant:
    """One plant for the sensor to watch."""
    return Plant.add(
        household_id=HouseholdId.new(),
        species_id=SpeciesId.new(),
        name="Fern",
        location=Location(value="Shelf"),
        now=NOW,
    )


def an_handler(plant: Plant | None, sensors: FakeSensors) -> AddSensorHandler:
    """The handler under test, over a fake unit of work and a frozen clock."""
    return AddSensorHandler(cast(UnitOfWork, FakeUnitOfWork(plant, sensors)), FakeClock())


async def test_the_server_mints_a_sensor_id_when_the_client_does_not() -> None:
    plant = a_plant()
    sensors = FakeSensors()
    handler = an_handler(plant, sensors)

    view = await handler.handle(AddSensorCommand(plant_id=plant.id))

    assert len(sensors.added) == 1
    assert view.sensor_id == sensors.added[0].id


async def test_the_identifier_the_client_owns_is_the_one_registered() -> None:
    plant = a_plant()
    sensors = FakeSensors()
    handler = an_handler(plant, sensors)
    owned = SensorId(uuid4())

    view = await handler.handle(AddSensorCommand(plant_id=plant.id, sensor_id=owned))

    assert view.sensor_id == owned
    assert sensors.added[0].id == owned
    assert sensors.added[0].plant_id == plant.id
    assert sensors.added[0].added_at == NOW


async def test_a_sensor_for_an_unknown_plant_is_refused() -> None:
    handler = an_handler(None, FakeSensors())

    with pytest.raises(NotFoundError):
        await handler.handle(AddSensorCommand(plant_id=PlantId(uuid4())))
