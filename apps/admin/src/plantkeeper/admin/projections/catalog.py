"""The Catalog read model.

Owns ``read_analytics.species`` and the denormalised ``plants.species_name`` the
admin shows next to each plant. Nothing publishes ``SpeciesUpdated`` yet — the
SpeciesSyncSaga does, in a later phase — so until then the column stays empty
rather than being read out of the write side's catalogue: a read model that joins
across the CQRS split would not be a read model.
"""

from __future__ import annotations

from typing import ClassVar

from plantkeeper.admin.projections.base import Projection, ProjectionHandler
from plantkeeper.admin.read_models.models import PlantReadModel, SpeciesReadModel
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.catalog.events import SpeciesUpdated
from plantkeeper.infrastructure.messaging.topics import CATALOG_EVENTS


class SpeciesProjection(Projection):
    """Consumes ``catalog.events`` into ``read_analytics.species``."""

    name = "catalog"
    topics = (CATALOG_EVENTS,)

    def on_species_updated(self, event: SpeciesUpdated) -> None:
        """Upsert the species and refresh the name carried by its plants."""
        SpeciesReadModel.objects.update_or_create(
            species_id=event.species_id.value,
            defaults={
                "scientific_name": event.scientific_name,
                "common_name": event.common_name,
                "watering_interval": event.watering_interval.value,
                "light_requirement": event.light_requirement.value,
                "version": event.version,
            },
        )
        PlantReadModel.objects.filter(species_id=event.species_id.value).update(
            species_name=event.common_name
        )

    handlers: ClassVar[dict[type[DomainEvent], ProjectionHandler]] = {
        SpeciesUpdated: on_species_updated,
    }
