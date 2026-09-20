"""Domain events published by the Catalog context."""

from __future__ import annotations

from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.domain.values import WateringInterval


class SpeciesSyncRequested(DomainEvent):
    """A catalogue synchronisation was requested.

    A trigger rather than a state change: the request carries no payload, and the
    daily scheduler or ``POST /api/v1/catalog/sync`` publishes it (Phase 4).
    """


class SpeciesUpdated(DomainEvent):
    """A species changed as a result of a Trefle synchronisation."""

    species_id: SpeciesId
    scientific_name: str
    common_name: str
    watering_interval: WateringInterval
    light_requirement: LightRequirement
    version: int


class SpeciesCacheInvalidated(DomainEvent):
    """The species cache entry for this species must be dropped (Phase 9)."""

    species_id: SpeciesId
