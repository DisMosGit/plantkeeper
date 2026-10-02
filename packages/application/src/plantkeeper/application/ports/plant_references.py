"""The reference a context keeps about a plant it does not own.

A consumer of care or journal facts needs one thing from the Garden context: which
household a plant belongs to, so a reminder can be addressed and a care fact can be
recognised as concerning a plant this context knows at all. Reading ``write_garden``
for it would make this context a second reader of another context's table —
forbidden by ``event-transport`` and invisible to the import rules, which is what
made it worth fixing.

So each consuming context keeps its own row, maintained from the Garden context's
events under its own consumer group. The row is a *copy*, not a truth: the Garden
context remains the only authority, and this port answers the two questions a
consumer actually has — "which household?" and "do I know this plant at all?".
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

from plantkeeper.domain.identifiers import HouseholdId, PlantId


class PlantReference(BaseModel):
    """One plant, as a consuming context recorded it from an event."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    plant_id: PlantId
    household_id: HouseholdId
    name: str
    location: str
    seen_at: datetime
    """When the event that last changed this row was raised.

    Kept so a consumer can order two events about the same plant that arrive out of
    order: the newer one is the one that wins, and an older one is ignored rather
    than reverting the row.
    """


@runtime_checkable
class PlantReferenceRepository(Protocol):
    """This context's own view of the Garden context's plants."""

    async def get(self, plant_id: PlantId) -> PlantReference | None:
        """Return the reference row, or ``None`` when this context never saw the plant.

        ``None`` is a normal answer, not an error: a fact can arrive before the
        ``PlantAdded`` that introduces it, and the documented rule for that is to
        drop the fact rather than invent a household for it.
        """
        ...

    async def upsert(self, reference: PlantReference) -> None:
        """Record or refresh a plant, ignoring an event older than the stored row."""
        ...

    async def remove(self, plant_id: PlantId) -> None:
        """Forget a plant the Garden context no longer has. Absent is not an error."""
        ...


@runtime_checkable
class NotificationPlantRefs(PlantReferenceRepository, Protocol):
    """The notifications context's own rows, as a dependency of its own.

    A name rather than a behaviour: both contexts' reference tables satisfy
    ``PlantReferenceRepository``, and Dishka keys a factory by its return type
    alone. Two providers returning that one port therefore collide — the later
    registration silently answers for both consumers — which is how a reminder
    ends up addressed from the journal's table. Each context names its own view so
    the graph cannot hand one context the other's rows, and ``mypy`` checks the
    consumer's parameter against that name.
    """


@runtime_checkable
class JournalPlantRefs(PlantReferenceRepository, Protocol):
    """The journal's own rows, as a dependency of its own.

    The twin of :class:`NotificationPlantRefs`, and separate for the same reason:
    one port type for two tables is one registration away from a context reading
    another context's rows.
    """
