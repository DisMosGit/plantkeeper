"""Integration tests for the two choreography consumers.

There is no saga transaction here: ``AdaptiveWateringSaga`` and ``MissedCareSaga``
are plain consumers, so the test drives ``consume`` exactly as the worker's handler
does — one claim, one handler, one commit — and then reads the write tables back
through a fresh session. The idempotency cases are the interesting ones: a
redelivery must not move a schedule twice, and ten seconds of telemetry must not
become ten notifications.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.requests.map import SagaMap
from cqrs.saga.storage.protocol import ISagaStorage
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.application.journal.consumer import JournalEntryConsumer
from plantkeeper.application.ports.sagas import MissedCareState, MissedCareWindow
from plantkeeper.application.sagas.adaptive_watering import AdaptiveWateringSaga
from plantkeeper.application.sagas.missed_care import GRACE_PERIOD, MissedCareSaga
from plantkeeper.domain.care.events import WateringCompleted, WateringDue
from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SensorId, SpeciesId
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.journal.values import JournalEntryType
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.domain.telemetry.events import SoilMoistureHigh, TelemetryReceived
from plantkeeper.domain.telemetry.sensor import MOISTURE_LOW_THRESHOLD
from plantkeeper.domain.values import LightLevel, Location, Moisture, Temperature, WateringInterval
from plantkeeper.infrastructure.persistence.models.notifications import NotificationModel
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel
from plantkeeper.infrastructure.persistence.repositories.care import (
    SqlAlchemyCareScheduleRepository,
)
from plantkeeper.infrastructure.persistence.repositories.sagas import (
    SqlAlchemyMissedCareWindowRepository,
)
from plantkeeper.infrastructure.persistence.saga_storage import SqlAlchemySagaStorage
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork

pytestmark = pytest.mark.integration

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
WEEK = timedelta(days=7)
GROUP = "test-consumer"


class FakeClock:
    """A clock the test moves by hand."""

    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        """Return the instant the test has set."""
        return self._now

    def advance(self, delta: timedelta) -> None:
        """Move the clock forward."""
        self._now += delta


class EmptyContainer:
    """A cqrs container for consumers that never dispatch a saga."""

    async def resolve[ResolvedT](self, type_: type[ResolvedT]) -> ResolvedT:
        """Never called by a choreography consumer."""
        raise KeyError(type_)


def a_dispatcher(storage: ISagaStorage) -> SagaDispatcher:
    """A dispatcher the choreography consumers accept and ignore."""
    return SagaDispatcher(SagaMap(), EmptyContainer(), storage)


async def seed_plant_with_schedule(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    next_watering_at: datetime,
    interval: timedelta = WEEK,
) -> tuple[HouseholdId, PlantId]:
    """Insert a household with one plant and a schedule due at ``next_watering_at``."""
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        async with uow:
            household = Household.create(name="Home")
            await uow.households.add(household)
            plant = Plant.add(
                household_id=household.id,
                species_id=SpeciesId.new(),
                name="Fern",
                location=Location(value="Shelf"),
                now=NOW,
            )
            household.add_plant(plant.id)
            await uow.plants.add(plant)
            schedule = CareSchedule.create(
                plant_id=plant.id,
                watering_interval=WateringInterval(value=interval),
                starts_at=next_watering_at,
                now=NOW,
            )
            await uow.care_schedules.add(schedule)
            await uow.commit()
            return household.id, plant.id


async def read_schedule(
    session_factory: async_sessionmaker[AsyncSession], plant_id: PlantId
) -> CareSchedule | None:
    """Read the plant's schedule back through its repository."""
    async with session_factory() as session:
        return await SqlAlchemyCareScheduleRepository(session, AggregateTracker()).get(plant_id)


async def read_window(
    session_factory: async_sessionmaker[AsyncSession], plant_id: PlantId
) -> MissedCareWindow | None:
    """Read the plant's grace window back through its repository."""
    async with session_factory() as session:
        return await SqlAlchemyMissedCareWindowRepository(session).get(plant_id)


async def outbox_event_names(session_factory: async_sessionmaker[AsyncSession]) -> list[str]:
    """Return the outbox in insertion order, as event names."""
    async with session_factory() as session:
        statement = select(OutboxModel).order_by(OutboxModel.id)
        models = (await session.execute(statement)).scalars()
        return [model.event_name for model in models]


async def notification_count(
    session_factory: async_sessionmaker[AsyncSession], notification_type: str
) -> int:
    """Count the stored notifications of one type."""
    async with session_factory() as session:
        statement = select(NotificationModel).where(
            NotificationModel.notification_type == notification_type
        )
        return len((await session.execute(statement)).scalars().all())


def a_telemetry_received(plant_id: PlantId, *, moisture: float) -> TelemetryReceived:
    """One reading, dry or not depending on ``moisture``."""
    return TelemetryReceived(
        sensor_id=SensorId.new(),
        plant_id=plant_id,
        recorded_at=NOW,
        moisture=Moisture(value=moisture),
        temperature=Temperature(value=21.0),
        light=LightLevel(value=800.0),
        occurred_at=NOW,
    )


# --- AdaptiveWateringSaga -----------------------------------------------------


async def test_dry_soil_pulls_a_far_away_watering_forward(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, plant_id = await seed_plant_with_schedule(session_factory, next_watering_at=NOW + WEEK)
    clock = FakeClock(NOW)
    storage = SqlAlchemySagaStorage(session_factory)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        saga = AdaptiveWateringSaga(uow, clock)
        event = a_telemetry_received(plant_id, moisture=MOISTURE_LOW_THRESHOLD - 5)
        assert await saga.consume(event, consumer_group=GROUP, dispatcher=a_dispatcher(storage))

    schedule = await read_schedule(session_factory, plant_id)
    assert schedule is not None
    assert schedule.next_watering_at == NOW
    assert schedule.version == 2
    assert "WateringRescheduled" in await outbox_event_names(session_factory)


async def test_dry_soil_leaves_a_watering_that_is_already_soon_alone(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    soon = NOW + timedelta(hours=6)
    _, plant_id = await seed_plant_with_schedule(session_factory, next_watering_at=soon)
    clock = FakeClock(NOW)
    storage = SqlAlchemySagaStorage(session_factory)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        saga = AdaptiveWateringSaga(uow, clock)
        await saga.consume(
            a_telemetry_received(plant_id, moisture=5.0),
            consumer_group=GROUP,
            dispatcher=a_dispatcher(storage),
        )

    schedule = await read_schedule(session_factory, plant_id)
    assert schedule is not None
    assert schedule.next_watering_at == soon
    assert schedule.version == 1


async def test_a_moist_reading_changes_nothing(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, plant_id = await seed_plant_with_schedule(session_factory, next_watering_at=NOW + WEEK)
    storage = SqlAlchemySagaStorage(session_factory)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        saga = AdaptiveWateringSaga(uow, FakeClock(NOW))
        await saga.consume(
            a_telemetry_received(plant_id, moisture=MOISTURE_LOW_THRESHOLD + 30),
            consumer_group=GROUP,
            dispatcher=a_dispatcher(storage),
        )

    schedule = await read_schedule(session_factory, plant_id)
    assert schedule is not None
    assert schedule.version == 1


async def test_overwatering_notifies_once_however_often_it_is_reported(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, plant_id = await seed_plant_with_schedule(session_factory, next_watering_at=NOW + WEEK)
    storage = SqlAlchemySagaStorage(session_factory)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        saga = AdaptiveWateringSaga(uow, FakeClock(NOW))
        for _ in range(3):
            await saga.consume(
                SoilMoistureHigh(
                    sensor_id=SensorId.new(),
                    plant_id=plant_id,
                    moisture=Moisture(value=95.0),
                    threshold=80.0,
                    occurred_at=NOW,
                ),
                consumer_group=GROUP,
                dispatcher=a_dispatcher(storage),
            )

    assert await notification_count(session_factory, NotificationType.SOIL_MOISTURE_HIGH.value) == 1


async def test_a_redelivered_reading_is_consumed_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, plant_id = await seed_plant_with_schedule(session_factory, next_watering_at=NOW + WEEK)
    storage = SqlAlchemySagaStorage(session_factory)
    event = a_telemetry_received(plant_id, moisture=5.0)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        saga = AdaptiveWateringSaga(uow, FakeClock(NOW))
        assert await saga.consume(event, consumer_group=GROUP, dispatcher=a_dispatcher(storage))
        assert not await saga.consume(event, consumer_group=GROUP, dispatcher=a_dispatcher(storage))

    names = await outbox_event_names(session_factory)
    assert names.count("WateringRescheduled") == 1


# --- MissedCareSaga -----------------------------------------------------------


def a_watering_due(plant_id: PlantId, *, due_at: datetime) -> WateringDue:
    """The fact that starts the grace window."""
    return WateringDue(plant_id=plant_id, due_at=due_at, occurred_at=NOW)


async def test_watering_due_opens_a_grace_window(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    household_id, plant_id = await seed_plant_with_schedule(
        session_factory, next_watering_at=NOW + WEEK
    )
    storage = SqlAlchemySagaStorage(session_factory)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        saga = MissedCareSaga(uow, FakeClock(NOW))
        assert await saga.consume(
            a_watering_due(plant_id, due_at=NOW),
            consumer_group=GROUP,
            dispatcher=a_dispatcher(storage),
        )

    window = await read_window(session_factory, plant_id)
    assert window is not None
    assert window.state is MissedCareState.PENDING
    assert window.household_id == household_id
    assert window.grace_deadline == NOW + GRACE_PERIOD


async def test_a_watering_inside_the_grace_period_satisfies_the_window(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, plant_id = await seed_plant_with_schedule(session_factory, next_watering_at=NOW + WEEK)
    clock = FakeClock(NOW)
    storage = SqlAlchemySagaStorage(session_factory)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        saga = MissedCareSaga(uow, clock)
        await saga.consume(
            a_watering_due(plant_id, due_at=NOW),
            consumer_group=GROUP,
            dispatcher=a_dispatcher(storage),
        )
        clock.advance(timedelta(hours=20))
        await saga.consume(
            WateringCompleted(
                plant_id=plant_id,
                completed_at=NOW + timedelta(hours=20),
                next_watering_at=NOW + timedelta(hours=20) + WEEK,
                occurred_at=NOW,
            ),
            consumer_group=GROUP,
            dispatcher=a_dispatcher(storage),
        )

    window = await read_window(session_factory, plant_id)
    assert window is not None
    assert window.state is MissedCareState.SATISFIED


async def test_an_expired_window_shifts_the_schedule(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The saga owns the schedule; the household is told by ``NotificationConsumer``."""
    due_at = NOW
    _, plant_id = await seed_plant_with_schedule(session_factory, next_watering_at=due_at)
    clock = FakeClock(NOW)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        saga = MissedCareSaga(uow, clock)
        assert await saga.escalate_overdue(now=NOW) == 0
        assert await saga.escalate_overdue(now=NOW) == 0

    # The schedule came due and a window opened, but only one WateringDue was
    # recorded for it however many ticks ran.
    names = await outbox_event_names(session_factory)
    assert names.count("WateringDue") == 1
    assert (await read_window(session_factory, plant_id)) is not None

    clock.advance(GRACE_PERIOD + timedelta(minutes=1))
    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        saga = MissedCareSaga(uow, clock)
        assert await saga.escalate_overdue(now=clock.now()) == 1

    schedule = await read_schedule(session_factory, plant_id)
    assert schedule is not None
    assert schedule.next_watering_at == due_at + WEEK
    window = await read_window(session_factory, plant_id)
    assert window is not None
    assert window.state is MissedCareState.MISSED
    names = await outbox_event_names(session_factory)
    assert names.count("CareMissed") == 1
    assert await notification_count(session_factory, NotificationType.CARE_MISSED.value) == 0


async def test_a_watering_after_the_deadline_does_not_erase_the_miss(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, plant_id = await seed_plant_with_schedule(session_factory, next_watering_at=NOW)
    clock = FakeClock(NOW)
    storage = SqlAlchemySagaStorage(session_factory)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        saga = MissedCareSaga(uow, clock)
        await saga.escalate_overdue(now=NOW)
        clock.advance(GRACE_PERIOD + timedelta(minutes=1))
        await saga.consume(
            WateringCompleted(
                plant_id=plant_id,
                completed_at=clock.now(),
                next_watering_at=clock.now() + WEEK,
                occurred_at=clock.now(),
            ),
            consumer_group=GROUP,
            dispatcher=a_dispatcher(storage),
        )
        assert await saga.escalate_overdue(now=clock.now()) == 1

    window = await read_window(session_factory, plant_id)
    assert window is not None
    assert window.state is MissedCareState.MISSED
    assert "CareMissed" in await outbox_event_names(session_factory)


# --- JournalEntryConsumer -----------------------------------------------------


def a_watering_completed(plant_id: PlantId) -> WateringCompleted:
    """One completion the journal should record."""
    return WateringCompleted(
        plant_id=plant_id,
        completed_at=NOW,
        next_watering_at=NOW + WEEK,
        occurred_at=NOW,
    )


async def test_a_completed_watering_is_journalled_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, plant_id = await seed_plant_with_schedule(session_factory, next_watering_at=NOW + WEEK)
    storage = SqlAlchemySagaStorage(session_factory)
    event = a_watering_completed(plant_id)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        consumer = JournalEntryConsumer(uow, FakeClock(NOW))
        assert await consumer.consume(event, consumer_group=GROUP, dispatcher=a_dispatcher(storage))
        # The ledger stops a redelivery of the same event.
        assert not await consumer.consume(
            event, consumer_group=GROUP, dispatcher=a_dispatcher(storage)
        )

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        stored = await uow.event_store.load_stream(plant_id)
        mirror = await uow.journal_entries.list_by_plant(plant_id)

    assert [row.version for row in stored] == [1]
    assert isinstance(stored[0].event, JournalEntryAdded)
    assert stored[0].event.entry_occurred_at == NOW
    assert [entry.entry_type for entry in mirror] == [JournalEntryType.WATERING]
    assert "JournalEntryAdded" in await outbox_event_names(session_factory)


async def test_a_rebuilt_consumer_group_does_not_journal_the_watering_again(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A reset offset plus a lost ledger is the case the derived entry id covers."""
    _, plant_id = await seed_plant_with_schedule(session_factory, next_watering_at=NOW + WEEK)
    storage = SqlAlchemySagaStorage(session_factory)
    event = a_watering_completed(plant_id)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        consumer = JournalEntryConsumer(uow, FakeClock(NOW))
        assert await consumer.consume(event, consumer_group=GROUP, dispatcher=a_dispatcher(storage))

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        rebuilt = JournalEntryConsumer(uow, FakeClock(NOW))
        assert await rebuilt.consume(
            event, consumer_group=f"{GROUP}-rebuilt", dispatcher=a_dispatcher(storage)
        )

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        stored = await uow.event_store.load_stream(plant_id)
        mirror = await uow.journal_entries.list_by_plant(plant_id)

    assert [row.version for row in stored] == [1]
    assert len(mirror) == 1


async def test_a_watering_for_an_unknown_plant_is_not_journalled(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    storage = SqlAlchemySagaStorage(session_factory)

    async with session_factory() as session:
        uow = SqlAlchemyUnitOfWork(session)
        consumer = JournalEntryConsumer(uow, FakeClock(NOW))
        assert await consumer.consume(
            a_watering_completed(PlantId.new()),
            consumer_group=GROUP,
            dispatcher=a_dispatcher(storage),
        )

    async with session_factory() as session:
        stored = await SqlAlchemyUnitOfWork(session).event_store.load_all()

    assert stored == []
