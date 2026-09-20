"""Plant endpoints."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, status

from plantkeeper.api.deps import Mediator
from plantkeeper.api.mediator import view_of
from plantkeeper.api.rest.routers.households import IdempotencyKey
from plantkeeper.api.rest.schemas.garden import (
    PlantCollectionResponse,
    PlantCreate,
    PlantResponse,
    PlantUpdate,
)
from plantkeeper.application.commands.garden import (
    AddPlantCommand,
    MovePlantCommand,
    RemovePlantCommand,
)
from plantkeeper.application.queries.garden import GetPlantQuery, ListPlantsQuery
from plantkeeper.application.views import CollectionView, PlantView
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SpeciesId
from plantkeeper.domain.values import Location

router = APIRouter(prefix="/api/v1/plants", tags=["plants"])


@router.post(
    "",
    response_model=PlantResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add a plant to a household",
)
async def add_plant(
    payload: PlantCreate,
    mediator: Mediator,
    idempotency_key: IdempotencyKey = None,
) -> PlantResponse:
    """Add a plant, exactly once per idempotency key.

    Returns 409 when the household is already at its cap of 50 plants, and 404
    when it does not exist.
    """
    command = AddPlantCommand(
        household_id=HouseholdId(payload.household_id),
        species_id=SpeciesId(payload.species_id),
        name=payload.name,
        location=Location(value=payload.location),
        idempotency_key=idempotency_key,
    )
    return PlantResponse.from_view(view_of(await mediator.send(command), PlantView))


@router.get("", response_model=PlantCollectionResponse, summary="List a household's plants")
async def list_plants(
    mediator: Mediator,
    household_id: Annotated[UUID, Query(description="The household whose plants to list.")],
    include_removed: Annotated[bool, Query(description="Include removed plants.")] = False,
) -> PlantCollectionResponse:
    """List the household's plants, oldest first."""
    query = ListPlantsQuery(
        household_id=HouseholdId(household_id),
        include_removed=include_removed,
    )
    view = view_of(await mediator.send(query), CollectionView[PlantView])
    return PlantCollectionResponse.from_view(view)


@router.get("/{plant_id}", response_model=PlantResponse, summary="Read a plant")
async def get_plant(plant_id: UUID, mediator: Mediator) -> PlantResponse:
    """Return one plant."""
    query = GetPlantQuery(plant_id=PlantId(plant_id))
    return PlantResponse.from_view(view_of(await mediator.send(query), PlantView))


@router.patch("/{plant_id}", response_model=PlantResponse, summary="Move a plant")
async def move_plant(plant_id: UUID, payload: PlantUpdate, mediator: Mediator) -> PlantResponse:
    """Move the plant to another location.

    Moving to the location the plant is already in answers with the unchanged
    plant and publishes nothing.
    """
    command = MovePlantCommand(
        plant_id=PlantId(plant_id), location=Location(value=payload.location)
    )
    return PlantResponse.from_view(view_of(await mediator.send(command), PlantView))


@router.delete("/{plant_id}", response_model=PlantResponse, summary="Remove a plant")
async def remove_plant(plant_id: UUID, mediator: Mediator) -> PlantResponse:
    """Remove the plant from the garden and publish ``PlantRemoved``.

    Returns 409 when the plant was already removed: the aggregate refuses to
    remove a plant twice, and reporting success would hide that.
    """
    command = RemovePlantCommand(plant_id=PlantId(plant_id))
    return PlantResponse.from_view(view_of(await mediator.send(command), PlantView))
