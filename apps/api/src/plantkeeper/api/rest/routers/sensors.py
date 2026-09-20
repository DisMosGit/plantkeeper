"""Sensor endpoints."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, status

from plantkeeper.api.deps import Mediator, view_of
from plantkeeper.api.rest.routers.households import IdempotencyKey
from plantkeeper.api.rest.schemas.telemetry import (
    SensorCollectionResponse,
    SensorCreate,
    SensorResponse,
)
from plantkeeper.application.commands.telemetry import AddSensorCommand, RemoveSensorCommand
from plantkeeper.application.queries.telemetry import ListSensorsQuery
from plantkeeper.application.views import CollectionView, SensorView
from plantkeeper.domain.identifiers import PlantId, SensorId

router = APIRouter(prefix="/api/v1/sensors", tags=["sensors"])


@router.post(
    "",
    response_model=SensorResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a sensor",
)
async def add_sensor(
    payload: SensorCreate,
    mediator: Mediator,
    idempotency_key: IdempotencyKey = None,
) -> SensorResponse:
    """Bind a sensor to an existing plant."""
    command = AddSensorCommand(
        plant_id=PlantId(payload.plant_id),
        sensor_id=SensorId(payload.sensor_id) if payload.sensor_id is not None else None,
        idempotency_key=idempotency_key,
    )
    return SensorResponse.from_view(view_of(await mediator.send(command), SensorView))


@router.get("", response_model=SensorCollectionResponse, summary="List a plant's sensors")
async def list_sensors(
    mediator: Mediator,
    plant_id: Annotated[UUID, Query(description="The plant whose sensors to list.")],
) -> SensorCollectionResponse:
    """List the sensors bound to one plant."""
    query = ListSensorsQuery(plant_id=PlantId(plant_id))
    view = view_of(await mediator.send(query), CollectionView[SensorView])
    return SensorCollectionResponse.from_view(view)


@router.delete("/{sensor_id}", response_model=SensorResponse, summary="Unregister a sensor")
async def remove_sensor(sensor_id: UUID, mediator: Mediator) -> SensorResponse:
    """Delete the sensor and answer with what was deleted."""
    command = RemoveSensorCommand(sensor_id=SensorId(sensor_id))
    return SensorResponse.from_view(view_of(await mediator.send(command), SensorView))
