"""The Garden read model.

Owns the garden-owned columns of ``read_analytics.plants``: identity, name,
location, the timestamps and the removal flag. ``species_name`` belongs to the
Catalog projection and ``next_watering_at`` to the Care projection, and
``update_or_create`` writes only the fields it is given, so those survive a
replay of ``PlantAdded``.
"""

from __future__ import annotations

from typing import ClassVar

from plantkeeper.admin.projections.base import Projection, ProjectionHandler
from plantkeeper.admin.read_models.models import PlantReadModel
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.garden.events import PlantAdded, PlantMoved, PlantOnboarded, PlantRemoved
from plantkeeper.infrastructure.messaging.topics import GARDEN_EVENTS


class GardenProjection(Projection):
    """Consumes ``garden.events`` into ``read_analytics.plants``."""

    name = "garden"
    topics = (GARDEN_EVENTS,)

    def on_plant_added(self, event: PlantAdded) -> None:
        """Create the plant row, or refresh the garden-owned columns it has.

        The row may already exist: a journal entry or a care schedule can be
        projected before the garden event, because the topics are consumed
        independently. In that case only the fields below are written.
        """
        PlantReadModel.objects.update_or_create(
            plant_id=event.plant_id.value,
            defaults={
                "household_id": event.household_id.value,
                "species_id": event.species_id.value,
                "name": event.name,
                "location": event.location.value,
                "added_at": event.added_at,
                "removed": False,
            },
        )

    def on_plant_moved(self, event: PlantMoved) -> None:
        """Record the new location."""
        PlantReadModel.objects.filter(plant_id=event.plant_id.value).update(
            location=event.location.value
        )

    def on_plant_removed(self, event: PlantRemoved) -> None:
        """Flag the plant as removed; the row stays, exactly as the API keeps it."""
        PlantReadModel.objects.filter(plant_id=event.plant_id.value).update(removed=True)

    def on_plant_onboarded(self, event: PlantOnboarded) -> None:
        """Record that the onboarding saga finished for this plant.

        The event also carries ``next_watering_at``, but Care owns that column and
        writes it from the schedule events.
        """
        PlantReadModel.objects.filter(plant_id=event.plant_id.value).update(
            onboarded_at=event.occurred_at
        )

    handlers: ClassVar[dict[type[DomainEvent], ProjectionHandler]] = {
        PlantAdded: on_plant_added,
        PlantMoved: on_plant_moved,
        PlantRemoved: on_plant_removed,
        PlantOnboarded: on_plant_onboarded,
    }
