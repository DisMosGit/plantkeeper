"""HTTP schemas for households and plants.

These are wire contracts, deliberately separate from the application's views:
the JSON a client sees can change (a field renamed, a duration expressed in
seconds) without touching a use case, and a view can gain an internal field
without leaking it.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from plantkeeper.application.views import (
    CollectionView,
    HouseholdView,
    PlantView,
)

_SCHEMA_CONFIG = ConfigDict(extra="forbid")


class HouseholdCreate(BaseModel):
    """The body of ``POST /api/v1/households``."""

    model_config = _SCHEMA_CONFIG

    name: str = Field(min_length=1, max_length=100)


class HouseholdResponse(BaseModel):
    """One household."""

    household_id: UUID
    name: str
    plant_count: int

    @classmethod
    def from_view(cls, view: HouseholdView) -> HouseholdResponse:
        """Map the view onto the wire format."""
        return cls(
            household_id=view.household_id.value,
            name=view.name,
            plant_count=view.plant_count,
        )


class PlantCreate(BaseModel):
    """The body of ``POST /api/v1/plants``."""

    model_config = _SCHEMA_CONFIG

    household_id: UUID
    species_id: UUID
    name: str = Field(min_length=1, max_length=200)
    location: str = Field(min_length=1, max_length=100)


class PlantUpdate(BaseModel):
    """The body of ``PATCH /api/v1/plants/{plant_id}``.

    Only the location may change: the Garden aggregate has no rename operation,
    so offering one here would be a lie in the OpenAPI document.
    """

    model_config = _SCHEMA_CONFIG

    location: str = Field(min_length=1, max_length=100)


class PlantResponse(BaseModel):
    """One plant."""

    plant_id: UUID
    household_id: UUID
    species_id: UUID
    name: str
    location: str
    added_at: datetime
    last_watered_at: datetime | None
    removed: bool

    @classmethod
    def from_view(cls, view: PlantView) -> PlantResponse:
        """Map the view onto the wire format."""
        return cls(
            plant_id=view.plant_id.value,
            household_id=view.household_id.value,
            species_id=view.species_id.value,
            name=view.name,
            location=view.location,
            added_at=view.added_at,
            last_watered_at=view.last_watered_at,
            removed=view.removed,
        )


class PlantCollectionResponse(BaseModel):
    """A list of plants."""

    items: list[PlantResponse]

    @classmethod
    def from_view(cls, view: CollectionView[PlantView]) -> PlantCollectionResponse:
        """Map each plant view onto the wire format."""
        return cls(items=[PlantResponse.from_view(item) for item in view.items])
