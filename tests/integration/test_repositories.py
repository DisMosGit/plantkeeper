"""Integration tests for the repositories and the unit of work.

These need a real Postgres: what is under test is the exact SQL — ``FOR UPDATE``
locks, the cross-schema join in ``list_due``, the partial index the outbox query
relies on — and a fake would test the fake instead.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.application.ports.idempotency import IdempotencyRecord
from plantkeeper.application.ports.outbox import OutboxMessage
from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SpeciesId
from plantkeeper.domain.values import Location, WateringInterval
from plantkeeper.infrastructure.persistence.models.garden import PlantModel
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel
from plantkeeper.infrastructure.persistence.repositories.outbox import (
    SqlAlchemyOutboxRepository,
)
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork

pytestmark = pytest.mark.integration

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def a_plant(household_id: HouseholdId, name: str = "Fern") -> Plant:
    """Return a plant that belongs to the given household."""
    return Plant.add(
        household_id=household_id,
        species_id=SpeciesId.new(),
        name=name,
        location=Location(value="Shelf"),
        now=NOW,
    )


async def count_rows(session: AsyncSession, model: type[object]) -> int:
    """Count the rows of a table."""
    statement = select(func.count()).select_from(model)
    return int((await session.execute(statement)).scalar_one())


async def test_a_plant_and_its_household_survive_a_commit(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            household = Household.create(name="Home")
            await uow.households.add(household)
            plant = a_plant(household.id)
            household.add_plant(plant.id)
            await uow.plants.add(plant)
            await uow.commit()
        household_id = household.id
        plant_id = plant.id

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        stored_household = await uow.households.get(household_id)
        stored_plant = await uow.plants.get(plant_id)

    assert stored_household is not None
    assert stored_household.name == "Home"
    assert stored_household.plant_ids == frozenset({plant_id})
    assert stored_plant is not None
    assert stored_plant.name == "Fern"
    assert stored_plant.location == Location(value="Shelf")


async def test_listing_plants_hides_removed_ones_unless_asked(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            household = Household.create(name="Home")
            await uow.households.add(household)
            kept = a_plant(household.id, name="Kept")
            removed = a_plant(household.id, name="Gone")
            removed.remove(now=NOW)
            await uow.plants.add(kept)
            await uow.plants.add(removed)
            await uow.commit()
        household_id = household.id

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        visible = await uow.plants.list_by_household(household_id)
        everything = await uow.plants.list_by_household(household_id, include_removed=True)

    assert [plant.name for plant in visible] == ["Kept"]
    assert {plant.name for plant in everything} == {"Kept", "Gone"}


async def test_a_schedule_is_versioned_across_saves(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    plant_id = PlantId.new()
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            schedule = CareSchedule.create(
                plant_id=plant_id,
                watering_interval=WateringInterval(value=timedelta(days=7)),
                starts_at=NOW,
                now=NOW,
            )
            await uow.care_schedules.add(schedule)
            await uow.commit()

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            locked = await uow.care_schedules.get_for_update(plant_id)
            assert locked is not None
            locked.complete_watering(now=NOW + timedelta(days=7), expected_version=1)
            await uow.care_schedules.save(locked)
            await uow.commit()

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        stored = await uow.care_schedules.get(plant_id)

    assert stored is not None
    assert stored.version == 2
    assert stored.next_watering_at == NOW + timedelta(days=14)


async def test_list_due_joins_the_plants_of_the_household(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            household = Household.create(name="Home")
            other = Household.create(name="Other")
            await uow.households.add(household)
            await uow.households.add(other)

            today = a_plant(household.id, name="Today")
            removed = a_plant(household.id, name="Removed")
            removed.remove(now=NOW)
            foreign = a_plant(other.id, name="Foreign")

            for plant, due_at in (
                (today, NOW + timedelta(hours=1)),
                (removed, NOW + timedelta(hours=1)),
                (foreign, NOW + timedelta(hours=1)),
            ):
                await uow.plants.add(plant)
                schedule = CareSchedule.create(
                    plant_id=plant.id,
                    watering_interval=WateringInterval(value=timedelta(days=7)),
                    starts_at=due_at,
                    now=NOW,
                )
                await uow.care_schedules.add(schedule)
            await uow.commit()
        household_id = household.id
        expected = today.id

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        due = await uow.care_schedules.list_due(household_id, NOW + timedelta(days=1))

    assert [schedule.plant_id for schedule in due] == [expected]


async def test_a_commit_writes_the_aggregate_and_its_event_together(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            household = Household.create(name="Home")
            await uow.households.add(household)
            plant = a_plant(household.id)
            await uow.plants.add(plant)
            await uow.commit()

    async with session_factory() as session:
        plants = await count_rows(session, PlantModel)
        rows = (await session.execute(select(OutboxModel))).scalars().all()

    assert plants == 1
    assert [row.event_name for row in rows] == ["PlantAdded"]
    assert rows[0].published_at is None
    assert rows[0].attempts == 0


async def test_a_failed_transaction_leaves_no_aggregate_and_no_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    with pytest.raises(RuntimeError, match="boom"):
        async with session_factory() as session:
            uow = SqlAlchemyUnitOfWork(session)
            async with uow:
                household = Household.create(name="Home")
                await uow.households.add(household)
                plant = a_plant(household.id)
                await uow.plants.add(plant)
                # Flush, so the inserts really reached the database before the
                # failure: nothing may survive the rollback.
                await session.flush()
                raise RuntimeError("boom")

    async with session_factory() as session:
        outbox = await session.execute(select(OutboxModel))
        rows = outbox.scalars().all()

    assert rows == []


async def test_the_outbox_drains_once_and_marks_what_it_published(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            household = Household.create(name="Home")
            await uow.households.add(household)
            await uow.plants.add(a_plant(household.id))
            await uow.commit()

    async with session_factory() as session:
        repository = SqlAlchemyOutboxRepository(session)
        pending = await repository.fetch_unpublished(10)
        assert len(pending) == 1
        message: OutboxMessage = pending[0]

        await repository.record_failure(message.outbox_id, "first attempt failed")
        await session.commit()

    async with session_factory() as session:
        repository = SqlAlchemyOutboxRepository(session)
        retried = await repository.fetch_unpublished(10)
        assert retried[0].attempts == 1

        await repository.mark_published(message.outbox_id)
        await session.commit()

    async with session_factory() as session:
        repository = SqlAlchemyOutboxRepository(session)
        assert await repository.fetch_unpublished(10) == []


async def test_a_dead_lettered_message_is_not_retried(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            household = Household.create(name="Home")
            await uow.households.add(household)
            await uow.plants.add(a_plant(household.id))
            await uow.commit()

    async with session_factory() as session:
        repository = SqlAlchemyOutboxRepository(session)
        message = (await repository.fetch_unpublished(10))[0]
        await repository.dead_letter(message.outbox_id, "gave up")
        await session.commit()

    async with session_factory() as session:
        repository = SqlAlchemyOutboxRepository(session)
        assert await repository.fetch_unpublished(10) == []


async def test_an_idempotency_record_is_stored_and_read_back(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    record = IdempotencyRecord(
        key="key-1",
        request_hash="a" * 64,
        status_code=201,
        response={"plant_id": str(PlantId.new())},
        created_at=NOW,
    )

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            assert await uow.idempotency.get(record.key) is None
            await uow.idempotency.remember(record)
            await uow.commit()

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        stored = await uow.idempotency.get(record.key)

    assert stored == record
