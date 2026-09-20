"""Integration tests for the Notifications context's producers.

``NotificationConsumer`` turns the care and telemetry events of Phase 8.3 into
``Notification`` rows, and ``NotificationPusher`` turns the resulting
``NotificationCreated`` into a nudge on the household's presence channel. Both are
driven exactly as the worker drives them — one claim, one handler, one commit —
and then read back through a fresh session.

The interesting cases are the ones that must *not* create anything: a redelivery,
a rebuilt consumer group, a trigger for an unknown plant, and the tenth dry reading
of the same afternoon.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime

import pytest
from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.requests.map import SagaMap
from cqrs.saga.storage.protocol import ISagaStorage
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from plantkeeper.application.notifications.consumer import NotificationConsumer
from plantkeeper.application.notifications.pusher import NotificationPusher
from plantkeeper.application.ports.notifications import (
    NotificationChannelError,
    NotificationSubscription,
)
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.care.events import CareMissed, WateringDue, WateringRescheduled
from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import HouseholdId, NotificationId, PlantId, SensorId, SpeciesId
from plantkeeper.domain.notifications.events import NotificationCreated
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.domain.telemetry.events import SoilMoistureLow, TemperatureAnomaly
from plantkeeper.domain.values import Location, Moisture, Temperature
from plantkeeper.infrastructure.persistence.models.notifications import NotificationModel
from plantkeeper.infrastructure.persistence.models.shared import OutboxModel
from plantkeeper.infrastructure.persistence.saga_storage import SqlAlchemySagaStorage
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork

pytestmark = pytest.mark.integration

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
GROUP = "test-notifications"


class FakeClock:
    """A clock the test sets."""

    def now(self) -> datetime:
        """Return the instant the test has chosen."""
        return NOW


class EmptyContainer:
    """A cqrs container for consumers that never dispatch a saga."""

    async def resolve[ResolvedT](self, type_: type[ResolvedT]) -> ResolvedT:
        """Never called by a choreography consumer."""
        raise KeyError(type_)


def a_dispatcher(storage: ISagaStorage) -> SagaDispatcher:
    """A dispatcher the choreography consumers accept and ignore."""
    return SagaDispatcher(SagaMap(), EmptyContainer(), storage)


class RecordingChannel:
    """A channel that records what was published, without any wiring."""

    def __init__(self) -> None:
        self.published: list[HouseholdId] = []

    async def publish(self, household_id: HouseholdId) -> None:
        """Record the nudge."""
        self.published.append(household_id)

    def subscribe(
        self, household_id: HouseholdId
    ) -> AbstractAsyncContextManager[NotificationSubscription]:
        """Unused: the pusher only publishes."""
        raise NotImplementedError


class FailingChannel:
    """A channel that is down, as Valkey would be."""

    async def publish(self, household_id: HouseholdId) -> None:
        """Fail the way an unreachable Valkey does."""
        raise NotificationChannelError("valkey is not answering")

    def subscribe(
        self, household_id: HouseholdId
    ) -> AbstractAsyncContextManager[NotificationSubscription]:
        """Unused: the pusher only publishes."""
        raise NotImplementedError


async def seed_plant(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[HouseholdId, PlantId]:
    """Insert a household with one plant and return their identifiers."""
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
            await uow.commit()
            return household.id, plant.id


async def consume(
    session_factory: async_sessionmaker[AsyncSession],
    event: DomainEvent,
    *,
    group: str = GROUP,
) -> bool:
    """Run one delivery through ``NotificationConsumer`` as the worker does."""
    storage = SqlAlchemySagaStorage(session_factory)
    async with session_factory() as session:
        consumer = NotificationConsumer(SqlAlchemyUnitOfWork(session), FakeClock())
        return await consumer.consume(event, consumer_group=group, dispatcher=a_dispatcher(storage))


async def stored_notifications(
    session_factory: async_sessionmaker[AsyncSession],
) -> list[NotificationModel]:
    """Every notification row, oldest first."""
    async with session_factory() as session:
        statement = select(NotificationModel).order_by(NotificationModel.created_at)
        return list((await session.execute(statement)).scalars().all())


async def outbox_event_names(session_factory: async_sessionmaker[AsyncSession]) -> list[str]:
    """Return the outbox in insertion order, as event names."""
    async with session_factory() as session:
        statement = select(OutboxModel).order_by(OutboxModel.id)
        return [row.event_name for row in (await session.execute(statement)).scalars().all()]


def a_watering_due(plant_id: PlantId) -> WateringDue:
    return WateringDue(plant_id=plant_id, due_at=NOW, occurred_at=NOW)


def a_watering_rescheduled(plant_id: PlantId) -> WateringRescheduled:
    return WateringRescheduled(
        plant_id=plant_id,
        previous_next_watering_at=NOW,
        next_watering_at=NOW,
        reason="telemetry: soil moisture below threshold",
        occurred_at=NOW,
    )


def a_care_missed(plant_id: PlantId) -> CareMissed:
    return CareMissed(plant_id=plant_id, next_watering_at=NOW, occurred_at=NOW)


def a_dry_reading(plant_id: PlantId, *, moisture: float = 12.0) -> SoilMoistureLow:
    return SoilMoistureLow(
        sensor_id=SensorId.new(),
        plant_id=plant_id,
        moisture=Moisture(value=moisture),
        threshold=30.0,
        occurred_at=NOW,
    )


def an_anomalous_temperature(plant_id: PlantId, *, temperature: float = 41.0) -> TemperatureAnomaly:
    return TemperatureAnomaly(
        sensor_id=SensorId.new(),
        plant_id=plant_id,
        temperature=Temperature(value=temperature),
        low_threshold=10.0,
        high_threshold=35.0,
        occurred_at=NOW,
    )


# --- NotificationConsumer -----------------------------------------------------


async def test_a_watering_due_becomes_a_watering_due_notification(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    household_id, plant_id = await seed_plant(session_factory)

    assert await consume(session_factory, a_watering_due(plant_id))

    [notification] = await stored_notifications(session_factory)
    assert notification.notification_type == NotificationType.WATERING_DUE.value
    assert notification.household_id == household_id.value
    assert notification.payload == {"plant_id": str(plant_id), "due_at": NOW.isoformat()}
    assert "NotificationCreated" in await outbox_event_names(session_factory)


async def test_a_care_missed_becomes_a_notification(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The notification ``MissedCareSaga`` no longer creates itself."""
    _, plant_id = await seed_plant(session_factory)

    assert await consume(session_factory, a_care_missed(plant_id))

    [notification] = await stored_notifications(session_factory)
    assert notification.notification_type == NotificationType.CARE_MISSED.value
    assert notification.payload == {
        "plant_id": str(plant_id),
        "next_watering_at": NOW.isoformat(),
    }


async def test_a_watering_rescheduled_becomes_a_notification(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, plant_id = await seed_plant(session_factory)

    assert await consume(session_factory, a_watering_rescheduled(plant_id))

    [notification] = await stored_notifications(session_factory)
    assert notification.notification_type == NotificationType.WATERING_RESCHEDULED.value
    assert notification.payload["reason"] == "telemetry: soil moisture below threshold"


async def test_a_dry_reading_notifies_once_while_it_is_unread(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A reading every ten seconds must not become a notification every ten seconds."""
    _, plant_id = await seed_plant(session_factory)

    for moisture in (12.0, 11.0, 9.0):
        assert await consume(session_factory, a_dry_reading(plant_id, moisture=moisture))

    [notification] = await stored_notifications(session_factory)
    assert notification.notification_type == NotificationType.SOIL_MOISTURE_LOW.value
    assert notification.payload["moisture"] == 12.0
    assert notification.payload["threshold"] == 30.0


async def test_a_temperature_anomaly_notifies_once_while_it_is_unread(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, plant_id = await seed_plant(session_factory)

    for temperature in (41.0, 42.0):
        assert await consume(
            session_factory, an_anomalous_temperature(plant_id, temperature=temperature)
        )

    [notification] = await stored_notifications(session_factory)
    assert notification.notification_type == NotificationType.TEMPERATURE_ANOMALY.value
    assert notification.payload["temperature"] == 41.0


async def test_a_redelivery_creates_nothing_new(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, plant_id = await seed_plant(session_factory)
    event = a_watering_due(plant_id)

    assert await consume(session_factory, event)
    assert not await consume(session_factory, event)

    assert len(await stored_notifications(session_factory)) == 1


async def test_a_rebuilt_consumer_group_creates_nothing_new(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A reset offset plus a lost ledger is what the derived identifier covers."""
    _, plant_id = await seed_plant(session_factory)
    event = a_watering_due(plant_id)

    assert await consume(session_factory, event)
    assert await consume(session_factory, event, group=f"{GROUP}-rebuilt")

    assert len(await stored_notifications(session_factory)) == 1


async def test_a_trigger_for_an_unknown_plant_creates_nothing(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    assert await consume(session_factory, a_watering_due(PlantId.new()))

    assert await stored_notifications(session_factory) == []
    assert "NotificationCreated" not in await outbox_event_names(session_factory)


# --- NotificationPusher -------------------------------------------------------


def a_notification_created() -> NotificationCreated:
    return NotificationCreated(
        notification_id=NotificationId.new(),
        household_id=HouseholdId.new(),
        notification_type=NotificationType.WATERING_DUE,
        payload={"plant_id": str(PlantId.new())},
        created_at=NOW,
        occurred_at=NOW,
    )


async def test_the_pusher_signals_the_notification_s_household(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    storage = SqlAlchemySagaStorage(session_factory)
    event = a_notification_created()
    channel = RecordingChannel()

    async with session_factory() as session:
        pusher = NotificationPusher(SqlAlchemyUnitOfWork(session), channel)
        assert await pusher.consume(event, consumer_group=GROUP, dispatcher=a_dispatcher(storage))

    assert channel.published == [event.household_id]


async def test_a_channel_failure_does_not_fail_the_delivery(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A nudge is best effort: the notification is already committed."""
    storage = SqlAlchemySagaStorage(session_factory)
    event = a_notification_created()

    async with session_factory() as session:
        pusher = NotificationPusher(SqlAlchemyUnitOfWork(session), FailingChannel())
        assert await pusher.consume(event, consumer_group=GROUP, dispatcher=a_dispatcher(storage))


async def test_a_nudge_for_an_unknown_event_is_ignored(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The topic carries other notification events; only creation is a nudge."""
    storage = SqlAlchemySagaStorage(session_factory)
    channel = RecordingChannel()

    async with session_factory() as session:
        pusher = NotificationPusher(SqlAlchemyUnitOfWork(session), channel)
        assert not await pusher.consume(
            a_watering_due(PlantId.new()),
            consumer_group=GROUP,
            dispatcher=a_dispatcher(storage),
        )

    assert channel.published == []
