"""The Catalog read model.

Owns ``read_analytics.species`` and the denormalised ``plants.species_name`` the
admin shows next to each plant. Its producer is the Trefle synchronisation
(Phase 9): ``SpeciesAdded`` for an entry that entered the catalogue, and
``SpeciesUpdated`` for one that changed — including the synchronisation's own
compensation, which restores a previous version through the same aggregate.
"""

from __future__ import annotations

from datetime import timedelta
from typing import ClassVar
from uuid import UUID

from plantkeeper.admin.projections.base import Projection, ProjectionHandler
from plantkeeper.admin.read_models.models import PlantReadModel, SpeciesReadModel
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.catalog.events import SpeciesAdded, SpeciesUpdated
from plantkeeper.infrastructure.messaging.topics import CATALOG_EVENTS


class SpeciesProjection(Projection):
    """Consumes ``catalog.events`` into ``read_analytics.species``."""

    name = "catalog"
    topics = (CATALOG_EVENTS,)

    def on_species_added(self, event: SpeciesAdded) -> None:
        """Upsert a species that just entered the catalogue."""
        self._upsert(
            species_id=event.species_id.value,
            scientific_name=event.scientific_name,
            common_name=event.common_name,
            watering_interval=event.watering_interval.value,
            light_requirement=event.light_requirement.value,
            version=event.version,
        )

    def on_species_updated(self, event: SpeciesUpdated) -> None:
        """Upsert the species and refresh the name carried by its plants."""
        self._upsert(
            species_id=event.species_id.value,
            scientific_name=event.scientific_name,
            common_name=event.common_name,
            watering_interval=event.watering_interval.value,
            light_requirement=event.light_requirement.value,
            version=event.version,
        )

    def _upsert(
        self,
        *,
        species_id: UUID,
        scientific_name: str,
        common_name: str,
        watering_interval: timedelta,
        light_requirement: str,
        version: int,
    ) -> None:
        """Write one catalogue row and the name its plants carry."""
        SpeciesReadModel.objects.update_or_create(
            species_id=species_id,
            defaults={
                "scientific_name": scientific_name,
                "common_name": common_name,
                "watering_interval": watering_interval,
                "light_requirement": light_requirement,
                "version": version,
            },
        )
        PlantReadModel.objects.filter(species_id=species_id).update(species_name=common_name)

    handlers: ClassVar[dict[type[DomainEvent], ProjectionHandler]] = {
        SpeciesAdded: on_species_added,
        SpeciesUpdated: on_species_updated,
    }
