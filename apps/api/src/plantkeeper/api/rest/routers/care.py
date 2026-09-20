"""Care endpoints: what is due today, and completing or skipping a watering."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from plantkeeper.api.deps import Mediator
from plantkeeper.api.mediator import view_of
from plantkeeper.api.rest.schemas.care import (
    CareScheduleCollectionResponse,
    CareScheduleResponse,
    WateringRequest,
)
from plantkeeper.application.commands.care import SkipWateringCommand, WaterPlantCommand
from plantkeeper.application.queries.care import GetTodayCareQuery
from plantkeeper.application.views import CareScheduleView, CollectionView
from plantkeeper.domain.identifiers import HouseholdId, PlantId

router = APIRouter(prefix="/api/v1/care", tags=["care"])


@router.get("/today", response_model=CareScheduleCollectionResponse, summary="What is due today")
async def get_today_care(
    mediator: Mediator,
    household_id: Annotated[UUID, Query(description="The household whose schedule to read.")],
) -> CareScheduleCollectionResponse:
    """Return the household's schedules that come due before midnight UTC."""
    query = GetTodayCareQuery(household_id=HouseholdId(household_id))
    view = view_of(await mediator.send(query), CollectionView[CareScheduleView])
    return CareScheduleCollectionResponse.from_view(view)


@router.post(
    "/{plant_id}/water",
    response_model=CareScheduleResponse,
    summary="Complete a watering",
)
async def water_plant(
    plant_id: UUID,
    mediator: Mediator,
    payload: WateringRequest | None = None,
) -> CareScheduleResponse:
    """Complete the watering and schedule the next one.

    Returns 409 when the plant was already watered less than an hour ago, which
    is the Garden aggregate's own rule.
    """
    command = WaterPlantCommand(
        plant_id=PlantId(plant_id),
        expected_version=payload.expected_version if payload is not None else None,
    )
    return CareScheduleResponse.from_view(view_of(await mediator.send(command), CareScheduleView))


@router.post("/{plant_id}/skip", response_model=CareScheduleResponse, summary="Skip a watering")
async def skip_watering(
    plant_id: UUID,
    mediator: Mediator,
    payload: WateringRequest | None = None,
) -> CareScheduleResponse:
    """Skip this watering and move to the following interval."""
    command = SkipWateringCommand(
        plant_id=PlantId(plant_id),
        expected_version=payload.expected_version if payload is not None else None,
    )
    return CareScheduleResponse.from_view(view_of(await mediator.send(command), CareScheduleView))
