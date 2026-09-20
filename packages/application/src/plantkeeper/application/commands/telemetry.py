"""Telemetry commands: registering and unregistering sensors."""

from __future__ import annotations

from plantkeeper.application.commands.base import Command, CommandHandler, IdempotentCommand
from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.idempotency import commit_create
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.views import SensorView
from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.telemetry.sensor import Sensor

CREATED = 201
"""The status a create use case answers with on the first and on a replayed call."""


class AddSensorCommand(IdempotentCommand):
    """Bind a sensor to a plant. Replayable through ``Idempotency-Key``.

    ``sensor_id`` is optional and normally left to the server: the identity of a
    sensor is the write side's to mint. It is accepted so that a caller which
    already owns the identifier — the IoT simulator derives a run's sensors from a
    base id it prints — can register exactly the sensor whose telemetry it will
    publish, instead of registering a sensor nobody will ever hear from.
    """

    plant_id: PlantId
    sensor_id: SensorId | None = None


class AddSensorHandler(CommandHandler[AddSensorCommand, SensorView]):
    """Register a sensor for an existing plant."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: AddSensorCommand) -> SensorView:
        """Register the sensor once, however often the client retries."""
        return await commit_create(
            uow=self._uow,
            clock=self._clock,
            command=command,
            view_type=SensorView,
            status_code=CREATED,
            operation=lambda: self._add(command),
        )

    async def _add(self, command: AddSensorCommand) -> SensorView:
        plant = await self._uow.plants.get(command.plant_id)
        if plant is None:
            raise NotFoundError(f"plant {command.plant_id} does not exist")
        sensor = Sensor.register(
            plant_id=command.plant_id,
            added_at=self._clock.now(),
            sensor_id=command.sensor_id,
        )
        await self._uow.sensors.add(sensor)
        return SensorView.from_domain(sensor)


class RemoveSensorCommand(Command):
    """Unregister a sensor."""

    sensor_id: SensorId


class RemoveSensorHandler(CommandHandler[RemoveSensorCommand, SensorView]):
    """Delete the sensor and return what was deleted."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: RemoveSensorCommand) -> SensorView:
        """Delete the sensor, or fail when it never existed."""
        async with self._uow:
            sensor = await self._uow.sensors.get(command.sensor_id)
            if sensor is None:
                raise NotFoundError(f"sensor {command.sensor_id} does not exist")
            view = SensorView.from_domain(sensor)
            await self._uow.sensors.delete(command.sensor_id)
            await self._uow.commit()
        return view
