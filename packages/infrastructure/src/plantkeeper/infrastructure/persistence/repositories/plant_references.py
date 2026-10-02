"""The reference rows, over whichever context's table the caller was given.

One implementation for the two tables rather than two near-identical classes: the
shape is the same because the fact is the same, and the only difference — which
schema the rows live in — is the model the caller passes in. A subclass per context
would be two places for the upsert rule to drift.

The model is typed as a :class:`PlantRefModel` protocol rather than as the concrete
class, because the caller genuinely passes one of two: ``Any`` would work and would
say nothing, while the protocol states exactly which columns this repository is
allowed to touch.
"""

from __future__ import annotations

from typing import Any, Protocol, cast

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.application.ports.plant_references import PlantReference
from plantkeeper.domain.identifiers import HouseholdId, PlantId


class PlantRefModel(Protocol):
    """A ``plant_refs`` table, whichever schema it lives in.

    The columns are ``Any`` because that is what SQLAlchemy's instrumented
    attributes are at this seam: on the class they are ``Mapped[T]`` descriptors, on
    a row they are ``T``, and this repository builds statements from the class and
    reads values from rows. ``Any`` is the honest type for "both", and it is
    confined to this protocol rather than spread through the methods.
    """

    plant_id: Any
    household_id: Any
    name: Any
    location: Any
    seen_at: Any


class SqlAlchemyPlantReferenceRepository:
    """The ``PlantReferenceRepository`` port over one context's ``plant_refs`` table."""

    def __init__(
        self,
        session: AsyncSession,
        model: type[PlantRefModel],
    ) -> None:
        self._session = session
        self._model = model

    async def get(self, plant_id: PlantId) -> PlantReference | None:
        """Return the reference row, or ``None`` when this context never saw it."""
        statement = select(self._model).where(self._model.plant_id == plant_id.value)
        row = (await self._session.execute(statement)).scalars().first()
        if row is None:
            return None
        return PlantReference(
            plant_id=PlantId(row.plant_id),
            household_id=HouseholdId(row.household_id),
            name=row.name,
            location=row.location,
            seen_at=row.seen_at,
        )

    async def upsert(self, reference: PlantReference) -> None:
        """Record or refresh a plant, ignoring an event older than the stored row.

        ``WHERE excluded.seen_at > plant_refs.seen_at`` is the out-of-order guard:
        a topic is ordered per partition key, but a rebuilt consumer group replays
        the whole topic, and a replay must not walk a moved plant back to where it
        used to be.

        The guard is written inline rather than built by a helper because
        ``excluded`` only exists inside the statement's scope: it is the pseudo-row
        the insert *would* have written, and SQLAlchemy binds it to the target table
        of the ``INSERT`` it belongs to. The ``cast`` is the seam where a protocol
        meets that pseudo-row, which is not a mapped class.
        """
        target = self._model
        excluded = cast("PlantRefModel", insert(target).excluded)
        statement = (
            insert(target)
            .values(
                plant_id=reference.plant_id.value,
                household_id=reference.household_id.value,
                name=reference.name,
                location=reference.location,
                seen_at=reference.seen_at,
            )
            .on_conflict_do_update(
                index_elements=[target.plant_id],
                set_={
                    "household_id": reference.household_id.value,
                    "name": reference.name,
                    "location": reference.location,
                    "seen_at": reference.seen_at,
                },
                where=excluded.seen_at > target.seen_at,
            )
        )
        await self._session.execute(statement)

    async def remove(self, plant_id: PlantId) -> None:
        """Forget a plant the Garden context no longer has."""
        await self._session.execute(
            delete(self._model).where(self._model.plant_id == plant_id.value)
        )
