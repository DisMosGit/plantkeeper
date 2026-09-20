"""Household endpoints: create a household and read it back.

A household needs an endpoint of its own before plants can be added: a plant
belongs to a household, so without one ``POST /plants`` would always be a 404.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, status

from plantkeeper.api.deps import Mediator, view_of
from plantkeeper.api.rest.schemas.garden import HouseholdCreate, HouseholdResponse
from plantkeeper.application.commands.garden import CreateHouseholdCommand
from plantkeeper.application.queries.garden import GetHouseholdQuery
from plantkeeper.application.views import HouseholdView
from plantkeeper.domain.identifiers import HouseholdId

router = APIRouter(prefix="/api/v1/households", tags=["households"])

IdempotencyKey = Annotated[
    str | None,
    Header(
        alias="Idempotency-Key",
        description="Replay-safe create: the same key returns the original response.",
    ),
]


@router.post(
    "",
    response_model=HouseholdResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a household",
)
async def create_household(
    payload: HouseholdCreate,
    mediator: Mediator,
    idempotency_key: IdempotencyKey = None,
) -> HouseholdResponse:
    """Create a household, exactly once per idempotency key."""
    command = CreateHouseholdCommand(name=payload.name, idempotency_key=idempotency_key)
    return HouseholdResponse.from_view(view_of(await mediator.send(command), HouseholdView))


@router.get("/{household_id}", response_model=HouseholdResponse, summary="Read a household")
async def get_household(household_id: UUID, mediator: Mediator) -> HouseholdResponse:
    """Return the household and how many plants it owns."""
    query = GetHouseholdQuery(household_id=HouseholdId(household_id))
    return HouseholdResponse.from_view(view_of(await mediator.send(query), HouseholdView))
