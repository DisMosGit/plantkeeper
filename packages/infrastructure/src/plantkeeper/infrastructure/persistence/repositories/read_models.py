"""The read-model query port over the read side's own database.

One adapter, four questions, all of them plain ``SELECT``s against
``read_analytics``' tables through the read-only mappings in
``persistence/models/read_models.py``. Nothing here rebuilds an aggregate: a read
model is already the projected answer, and the aggregate behind it lives in the
write side's event stream, which a list query has no business replaying.

Each method opens a short session of its own from the read engine's pool rather
than sharing the request's write session. That is the point of the split: a query
that never touches the write side cannot be made slow — or made to fail — by it,
and no read connection is held while a handler does anything else.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.application.ports.read_models import (
    CareScheduleRow,
    JournalTimelineRow,
    PlantListRow,
    SpeciesListRow,
)
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.identifiers import (
    HouseholdId,
    JournalEntryId,
    PlantId,
    SpeciesId,
)
from plantkeeper.domain.journal.values import JournalEntryType
from plantkeeper.infrastructure.persistence.models.read_models import (
    ReadCareScheduleModel,
    ReadJournalEntryModel,
    ReadPlantModel,
    ReadSpeciesModel,
)


class SqlAlchemyReadModelReader:
    """The ``ReadModelReader`` port over a read-only SQLAlchemy engine."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = session_factory

    async def list_plants(
        self, household_id: HouseholdId, *, include_removed: bool
    ) -> list[PlantListRow]:
        """List a household's plants, oldest first.

        Only rows the Garden projection has completed are listed: a placeholder row
        left behind by a care or journal event carries no name, location or
        ``added_at``, and rendering it would mean answering with a half-plant. The
        ``last_watered_at`` the view reports is the care model's column, joined in
        because a plant list that had to ask the write side for it would not be a
        read-model answer at all.
        """
        statement = (
            select(ReadPlantModel, ReadCareScheduleModel.last_watered_at)
            .outerjoin(
                ReadCareScheduleModel,
                ReadCareScheduleModel.plant_id == ReadPlantModel.plant_id,
            )
            .where(
                ReadPlantModel.household_id == household_id.value,
                ReadPlantModel.species_id.is_not(None),
                ReadPlantModel.name.is_not(None),
                ReadPlantModel.location.is_not(None),
                ReadPlantModel.added_at.is_not(None),
            )
            .order_by(ReadPlantModel.added_at, ReadPlantModel.plant_id)
        )
        if not include_removed:
            statement = statement.where(ReadPlantModel.removed.is_(False))

        async with self._sessions() as session:
            rows = (await session.execute(statement)).all()
        return [_plant_row(plant, last_watered_at) for plant, last_watered_at in rows]

    async def list_due_care(
        self, household_id: HouseholdId, until: datetime
    ) -> list[CareScheduleRow]:
        """List a household's schedules due at or before ``until``, soonest first.

        The join to ``plants`` is what excludes removed plants, exactly as the
        write-side query does; both tables are read-side tables, so the join stays
        inside the read model.
        """
        statement = (
            select(ReadCareScheduleModel)
            .join(ReadPlantModel, ReadPlantModel.plant_id == ReadCareScheduleModel.plant_id)
            .where(
                ReadPlantModel.household_id == household_id.value,
                ReadPlantModel.removed.is_(False),
                ReadCareScheduleModel.next_watering_at <= until,
            )
            .order_by(ReadCareScheduleModel.next_watering_at, ReadCareScheduleModel.plant_id)
        )
        async with self._sessions() as session:
            models = (await session.execute(statement)).scalars().all()
        return [_care_schedule_row(model) for model in models]

    async def list_species(self) -> list[SpeciesListRow]:
        """List every catalogue entry, by scientific name."""
        statement = select(ReadSpeciesModel).order_by(
            ReadSpeciesModel.scientific_name, ReadSpeciesModel.species_id
        )
        async with self._sessions() as session:
            models = (await session.execute(statement)).scalars().all()
        return [_species_row(model) for model in models]

    async def plant_exists(self, plant_id: PlantId) -> bool:
        """Whether the read side knows the plant."""
        statement = select(ReadPlantModel.plant_id).where(ReadPlantModel.plant_id == plant_id.value)
        async with self._sessions() as session:
            return (await session.execute(statement)).scalar_one_or_none() is not None

    async def list_journal_entries(self, plant_id: PlantId) -> list[JournalTimelineRow]:
        """List one plant's journal entries in the order they were recorded."""
        statement: Select[tuple[ReadJournalEntryModel]] = (
            select(ReadJournalEntryModel)
            .where(ReadJournalEntryModel.plant_id == plant_id.value)
            .order_by(ReadJournalEntryModel.recorded_at, ReadJournalEntryModel.entry_id)
        )
        async with self._sessions() as session:
            models = (await session.execute(statement)).scalars().all()
        return [_journal_entry_row(model) for model in models]


def _plant_row(plant: ReadPlantModel, last_watered_at: datetime | None) -> PlantListRow:
    """Turn one mapped row into the port's row.

    The assertions restate what the query's ``WHERE`` clause already guarantees —
    the four garden-owned columns are non-null for a listed plant — so the type
    checker can see it without a cast.
    """
    assert plant.species_id is not None  # WHERE species_id IS NOT NULL
    assert plant.name is not None  # WHERE name IS NOT NULL
    assert plant.location is not None  # WHERE location IS NOT NULL
    assert plant.added_at is not None  # WHERE added_at IS NOT NULL
    assert plant.household_id is not None  # WHERE household_id = :household_id
    return PlantListRow(
        plant_id=PlantId(plant.plant_id),
        household_id=HouseholdId(plant.household_id),
        species_id=SpeciesId(plant.species_id),
        name=plant.name,
        location=plant.location,
        added_at=plant.added_at,
        last_watered_at=last_watered_at,
        removed=plant.removed,
        next_watering_at=plant.next_watering_at,
    )


def _care_schedule_row(model: ReadCareScheduleModel) -> CareScheduleRow:
    """Turn one mapped schedule into the port's row."""
    return CareScheduleRow(
        plant_id=PlantId(model.plant_id),
        watering_interval=model.watering_interval,
        next_watering_at=model.next_watering_at,
        last_watered_at=model.last_watered_at,
        version=model.version,
    )


def _species_row(model: ReadSpeciesModel) -> SpeciesListRow:
    """Turn one mapped species into the port's row.

    ``light_requirement`` is stored as text and validated back into the domain's
    enum here: a value the domain does not know is a contract violation the read
    side should fail loudly on, not pass through as an opaque string.
    """
    return SpeciesListRow(
        species_id=SpeciesId(model.species_id),
        scientific_name=model.scientific_name,
        common_name=model.common_name,
        watering_interval=model.watering_interval,
        light_requirement=LightRequirement(model.light_requirement),
        version=model.version,
    )


def _journal_entry_row(model: ReadJournalEntryModel) -> JournalTimelineRow:
    """Turn one mapped journal entry into the port's row."""
    return JournalTimelineRow(
        entry_id=JournalEntryId(model.entry_id),
        plant_id=PlantId(model.plant_id),
        entry_type=JournalEntryType(model.entry_type),
        occurred_at=model.occurred_at,
        note=model.note,
    )
