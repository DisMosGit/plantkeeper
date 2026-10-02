"""Keeping a consuming context's own row about a plant in step with the Garden context.

Two consumers need one fact the Garden context owns — which household a plant
belongs to — and neither may read ``write_garden`` for it (``event-transport``). The
notifications context addresses a reminder with it; the journal records it so a
watering can be recognised as concerning a plant this context knows. Each keeps a
``plant_refs`` row of its own, filled from
``PlantAdded``/``PlantMoved``/``PlantRemoved`` under its own consumer group.

The rule is one rule, so it is one function rather than two implementations that
would drift:

* an addition records the plant;
* a move refreshes the location of a plant this context already knows, and a move
  for one it has never seen is dropped — the row would otherwise name a household
  nobody told this context about;
* a removal forgets the plant, after which a fact about it is dropped too.

Dropping is the documented answer for a fact that arrives before the plant it
concerns: this context holds no household for it, and inventing one — or holding the
fact aside until the introduction turns up — would make the reference table a queue
with an unbounded lifetime rather than a copy of a fact another context owns.
"""

from __future__ import annotations

import logging

from plantkeeper.application.ports.plant_references import (
    PlantReference,
    PlantReferenceRepository,
)
from plantkeeper.domain.garden.events import PlantAdded, PlantMoved, PlantRemoved
from plantkeeper.domain.identifiers import HouseholdId

logger = logging.getLogger(__name__)

GardenPlantEvent = PlantAdded | PlantMoved | PlantRemoved
"""The Garden facts a reference row is maintained from."""


async def maintain_plant_reference(
    plant_refs: PlantReferenceRepository,
    event: GardenPlantEvent,
    *,
    context: str,
) -> None:
    """Record, refresh or forget one plant from the event that reports it.

    ``context`` names the consuming context in the log line only. The two tables
    have the same shape, so a shared message that could not say which context
    ignored an event would be the kind of ambiguity this module exists to remove.
    """
    if isinstance(event, PlantRemoved):
        await plant_refs.remove(event.plant_id)
        return
    facts = await _garden_facts(plant_refs, event)
    if facts is None:
        logger.info(
            "%s: %s for plant %s this context has not seen; reference unchanged",
            context,
            type(event).__name__,
            event.plant_id,
        )
        return
    household_id, name, location = facts
    await plant_refs.upsert(
        PlantReference(
            plant_id=event.plant_id,
            household_id=household_id,
            name=name,
            location=location,
            seen_at=event.occurred_at,
        )
    )


async def _garden_facts(
    plant_refs: PlantReferenceRepository, event: PlantAdded | PlantMoved
) -> tuple[HouseholdId, str, str] | None:
    """The household, name and location an addition carries or a move preserves.

    ``PlantAdded`` carries all three. ``PlantMoved`` carries only the new location,
    so the rest comes from the row this context already has, and a move for a plant
    it has never seen answers ``None`` — dropped rather than recorded against a
    household nobody named.
    """
    if isinstance(event, PlantAdded):
        return event.household_id, event.name, event.location.value
    known = await plant_refs.get(event.plant_id)
    if known is None:
        return None
    return known.household_id, known.name, event.location.value
