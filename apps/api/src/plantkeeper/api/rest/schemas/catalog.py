"""HTTP schemas for the species catalogue."""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel

from plantkeeper.application.views import CollectionView, SpeciesView, SyncRequestedView
from plantkeeper.domain.catalog.values import LightRequirement


class SpeciesResponse(BaseModel):
    """One catalogue entry."""

    species_id: UUID
    scientific_name: str
    common_name: str
    watering_interval_seconds: int
    light_requirement: LightRequirement
    version: int

    @classmethod
    def from_view(cls, view: SpeciesView) -> SpeciesResponse:
        """Map the view onto the wire format."""
        return cls(
            species_id=view.species_id.value,
            scientific_name=view.scientific_name,
            common_name=view.common_name,
            watering_interval_seconds=int(view.watering_interval.total_seconds()),
            light_requirement=view.light_requirement,
            version=view.version,
        )


class SpeciesCollectionResponse(BaseModel):
    """A list of catalogue entries."""

    items: list[SpeciesResponse]

    @classmethod
    def from_view(cls, view: CollectionView[SpeciesView]) -> SpeciesCollectionResponse:
        """Map each species view onto the wire format."""
        return cls(items=[SpeciesResponse.from_view(item) for item in view.items])


class SyncRequestedResponse(BaseModel):
    """The answer to a synchronisation request.

    The work itself happens in another process: the request only publishes
    ``SpeciesSyncRequested``, which is why the status is 202 and not 200.
    """

    requested: bool

    @classmethod
    def from_view(cls, view: SyncRequestedView) -> SyncRequestedResponse:
        """Map the view onto the wire format."""
        return cls(requested=view.requested)
