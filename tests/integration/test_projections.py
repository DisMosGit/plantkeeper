"""Integration tests for the read side's projections.

They run against the migrated ``read_analytics`` schema in the testcontainers
Postgres, and they call the projections the way the consumer does:
``Projection.apply`` inside a transaction, with the ledger in the same
transaction. Django's ORM is synchronous, so the tests are too — except the two
that prove the async wrapper the FastStream subscriber uses.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, cast

import pytest
from aiokafka import ConsumerRecord
from faststream.kafka import KafkaMessage
from faststream.kafka.message import ConsumerProtocol

from plantkeeper.admin.projections import ALL_PROJECTIONS
from plantkeeper.admin.projections.base import Projection, ProjectionHandler, apply_event
from plantkeeper.admin.projections.subscriber import ProjectionSubscriber, build_handler
from plantkeeper.admin.read_models.models import (
    CareReadModel,
    JournalReadModel,
    NotificationReadModel,
    PlantReadModel,
    ProcessedEvent,
    SpeciesReadModel,
)
from plantkeeper.admin.read_telemetry.models import SensorLatest, TelemetryRollup
from plantkeeper.application.delivery import FailurePolicy
from plantkeeper.application.ports.dead_letter import DeadLetterPublisher, DeliveryDeadLetter
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.care.events import (
    CareMissed,
    CareScheduleCreated,
    CareSkipped,
    WateringCompleted,
    WateringDue,
    WateringRescheduled,
)
from plantkeeper.domain.catalog.events import (
    SpeciesAdded,
    SpeciesCacheInvalidated,
    SpeciesSyncRequested,
    SpeciesUpdated,
)
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.garden.errors import PlantAlreadyRemovedError
from plantkeeper.domain.garden.events import PlantAdded, PlantMoved, PlantOnboarded, PlantRemoved
from plantkeeper.domain.identifiers import (
    HouseholdId,
    JournalEntryId,
    NotificationId,
    PlantId,
    SensorId,
    SpeciesId,
)
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.journal.values import JournalEntryType
from plantkeeper.domain.notifications.events import NotificationCreated, NotificationRead
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.domain.saga.events import (
    SagaCompensated,
    SagaCompleted,
    SagaFailed,
    SagaParked,
    SagaRetrying,
    SagaStarted,
)
from plantkeeper.domain.telemetry.events import (
    SensorOffline,
    SoilMoistureHigh,
    SoilMoistureLow,
    TelemetryReceived,
    TemperatureAnomaly,
)
from plantkeeper.domain.values import LightLevel, Location, Moisture, Temperature, WateringInterval
from plantkeeper.infrastructure.messaging.topics import (
    EVENT_TOPICS,
    GARDEN_EVENTS,
    HEADER_EVENT_ID,
    HEADER_EVENT_NAME,
)

pytestmark = pytest.mark.integration

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
LATER = NOW + timedelta(days=7)
WEEK = timedelta(days=7)

PLANT = PlantId.new()
HOUSEHOLD = HouseholdId.new()
SPECIES = SpeciesId.new()

PROJECTIONS: dict[str, Projection] = {projection.name: projection for projection in ALL_PROJECTIONS}

UNPROJECTED_EVENTS: set[type[DomainEvent]] = {
    # Care: the schedule is marked due, but no read model shows that yet.
    WateringDue,
    # Catalog: a trigger, and a cache concern rather than a read model.
    SpeciesSyncRequested,
    SpeciesCacheInvalidated,
    # Saga/system: the read side does not project process-manager progress.
    SagaStarted,
    SagaCompleted,
    SagaFailed,
    SagaCompensated,
    SagaRetrying,
    SagaParked,
}
"""Catalogue events no projection consumes, named so the gap is a decision.

Telemetry is deliberately *not* here: ``TelemetryRollupProjection`` consumes
``telemetry.events`` under a consumer group of its own, so every telemetry event —
the reading, the three threshold alerts and the silence — has a read-side effect
(``docs/telemetry.md``)."""


def group(name: str) -> str:
    """The consumer group a test uses for a projection."""
    return f"test-{name}"


def plant_added(*, name: str = "Fern", location: str = "Shelf") -> PlantAdded:
    """``PlantAdded`` for the shared plant."""
    return PlantAdded(
        plant_id=PLANT,
        household_id=HOUSEHOLD,
        species_id=SPECIES,
        name=name,
        location=Location(value=location),
        added_at=NOW,
    )


def care_schedule_created(*, next_watering_at: datetime = LATER) -> CareScheduleCreated:
    """``CareScheduleCreated`` for the shared plant."""
    return CareScheduleCreated(
        plant_id=PLANT,
        watering_interval=WateringInterval(value=WEEK),
        next_watering_at=next_watering_at,
    )


def species_updated(*, common_name: str = "Boston fern", version: int = 2) -> SpeciesUpdated:
    """``SpeciesUpdated`` for the shared species."""
    return SpeciesUpdated(
        species_id=SPECIES,
        scientific_name="Nephrolepis exaltata",
        common_name=common_name,
        watering_interval=WateringInterval(value=WEEK),
        light_requirement=LightRequirement.MEDIUM,
        version=version,
    )


def species_added(*, common_name: str = "Aloe", version: int = 1) -> SpeciesAdded:
    """``SpeciesAdded`` for the shared species."""
    return SpeciesAdded(
        species_id=SPECIES,
        scientific_name="Aloe vera",
        common_name=common_name,
        watering_interval=WateringInterval(value=WEEK),
        light_requirement=LightRequirement.MEDIUM,
        version=version,
    )


def journal_entry(*, note: str | None = "Watered") -> JournalEntryAdded:
    """``JournalEntryAdded`` for the shared plant."""
    return JournalEntryAdded(
        entry_id=JournalEntryId.new(),
        plant_id=PLANT,
        entry_type=JournalEntryType.WATERING,
        note=note,
        entry_occurred_at=NOW,
    )


# --- The registry -------------------------------------------------------------


def test_every_handled_event_travels_on_a_topic_its_projection_subscribes_to() -> None:
    """A handler whose event never arrives on the subscribed topic is dead code."""
    for projection in ALL_PROJECTIONS:
        for event_type in projection.handlers:
            assert EVENT_TOPICS[event_type] in projection.topics, projection.name


def test_every_projection_consumes_something() -> None:
    for projection in ALL_PROJECTIONS:
        assert projection.topics, projection.name
        assert projection.handlers, projection.name


def test_consumer_groups_are_distinct() -> None:
    """One group per projection: each owns its offsets and its idempotency domain."""
    names = [projection.name for projection in ALL_PROJECTIONS]
    assert len(names) == len(set(names))


def test_the_projected_catalogue_is_accounted_for() -> None:
    handled = {event for projection in ALL_PROJECTIONS for event in projection.handlers}
    assert handled & UNPROJECTED_EVENTS == set()
    assert handled | UNPROJECTED_EVENTS == set(EVENT_TOPICS)


# --- Garden -------------------------------------------------------------------


def test_plant_added_creates_the_row(read_side_database: str) -> None:
    event = plant_added()

    assert PROJECTIONS["garden"].apply(event, consumer_group=group("garden")) is True

    row = PlantReadModel.objects.get(plant_id=PLANT.value)
    assert row.name == "Fern"
    assert row.location == "Shelf"
    assert row.household_id == HOUSEHOLD.value
    assert row.species_id == SPECIES.value
    assert row.added_at == NOW
    assert row.removed is False
    assert row.next_watering_at is None


def test_plant_moved_updates_the_location(read_side_database: str) -> None:
    projection = PROJECTIONS["garden"]
    projection.apply(plant_added(), consumer_group=group("garden"))
    projection.apply(
        PlantMoved(
            plant_id=PLANT,
            previous_location=Location(value="Shelf"),
            location=Location(value="Window"),
        ),
        consumer_group=group("garden"),
    )

    assert PlantReadModel.objects.get(plant_id=PLANT.value).location == "Window"


def test_plant_removed_flags_the_row_without_deleting_it(read_side_database: str) -> None:
    projection = PROJECTIONS["garden"]
    projection.apply(plant_added(), consumer_group=group("garden"))
    projection.apply(PlantRemoved(plant_id=PLANT, removed_at=NOW), consumer_group=group("garden"))

    row = PlantReadModel.objects.get(plant_id=PLANT.value)
    assert row.removed is True
    assert row.name == "Fern"


def test_plant_onboarded_records_when_it_finished(read_side_database: str) -> None:
    projection = PROJECTIONS["garden"]
    projection.apply(plant_added(), consumer_group=group("garden"))
    onboarding = PlantOnboarded(
        plant_id=PLANT,
        household_id=HOUSEHOLD,
        species_id=SPECIES,
        next_watering_at=LATER,
        occurred_at=LATER,
    )

    projection.apply(onboarding, consumer_group=group("garden"))

    row = PlantReadModel.objects.get(plant_id=PLANT.value)
    assert row.onboarded_at == LATER
    # Care owns the next watering, so the garden event does not write it.
    assert row.next_watering_at is None


def test_garden_leaves_the_columns_it_does_not_own_alone(read_side_database: str) -> None:
    """A corrected ``PlantAdded`` must not blank the catalog's and care's columns."""
    garden = PROJECTIONS["garden"]
    garden.apply(plant_added(), consumer_group=group("garden"))
    PROJECTIONS["care"].apply(care_schedule_created(), consumer_group=group("care"))
    PROJECTIONS["catalog"].apply(species_updated(), consumer_group=group("catalog"))

    garden.apply(plant_added(name="Fern II"), consumer_group=group("garden"))

    row = PlantReadModel.objects.get(plant_id=PLANT.value)
    assert row.name == "Fern II"
    assert row.next_watering_at == LATER
    assert row.species_name == "Boston fern"


# --- Care ---------------------------------------------------------------------


def test_care_schedule_created_writes_the_schedule_and_the_plant_row(
    read_side_database: str,
) -> None:
    event = care_schedule_created()

    assert PROJECTIONS["care"].apply(event, consumer_group=group("care")) is True

    schedule = CareReadModel.objects.get(plant_id=PLANT.value)
    assert schedule.watering_interval == WEEK
    assert schedule.next_watering_at == LATER
    assert schedule.version == 1
    assert schedule.last_watered_at is None
    # The garden event has not been projected yet; the partial row is deliberate.
    assert PlantReadModel.objects.get(plant_id=PLANT.value).next_watering_at == LATER


def test_watering_completed_moves_the_schedule_and_records_the_moment(
    read_side_database: str,
) -> None:
    projection = PROJECTIONS["care"]
    projection.apply(care_schedule_created(next_watering_at=NOW), consumer_group=group("care"))

    completed = NOW + timedelta(days=1)
    projection.apply(
        WateringCompleted(plant_id=PLANT, completed_at=completed, next_watering_at=LATER),
        consumer_group=group("care"),
    )

    schedule = CareReadModel.objects.get(plant_id=PLANT.value)
    assert schedule.last_watered_at == completed
    assert schedule.next_watering_at == LATER
    assert PlantReadModel.objects.get(plant_id=PLANT.value).next_watering_at == LATER


@pytest.mark.parametrize(
    "event",
    [
        WateringRescheduled(
            plant_id=PLANT, previous_next_watering_at=NOW, next_watering_at=LATER, reason="dry soil"
        ),
        CareSkipped(plant_id=PLANT, skipped_at=NOW, next_watering_at=LATER),
        CareMissed(plant_id=PLANT, next_watering_at=LATER),
    ],
    ids=["rescheduled", "skipped", "missed"],
)
def test_every_care_event_that_moves_the_schedule_moves_the_read_model(
    event: DomainEvent, read_side_database: str
) -> None:
    """A skip or a miss moves the next watering just as a reschedule does."""
    projection = PROJECTIONS["care"]
    projection.apply(care_schedule_created(next_watering_at=NOW), consumer_group=group("care"))

    projection.apply(event, consumer_group=group("care"))

    assert CareReadModel.objects.get(plant_id=PLANT.value).next_watering_at == LATER
    assert PlantReadModel.objects.get(plant_id=PLANT.value).next_watering_at == LATER


def test_an_existing_schedule_is_refreshed_without_losing_its_version(
    read_side_database: str,
) -> None:
    """A second schedule event updates the interval, but the version is the write side's."""
    projection = PROJECTIONS["care"]
    projection.apply(care_schedule_created(next_watering_at=NOW), consumer_group=group("care"))
    CareReadModel.objects.filter(plant_id=PLANT.value).update(version=4)

    projection.apply(care_schedule_created(next_watering_at=LATER), consumer_group=group("care"))

    schedule = CareReadModel.objects.get(plant_id=PLANT.value)
    assert schedule.next_watering_at == LATER
    assert schedule.watering_interval == WEEK
    assert schedule.version == 4


# --- Catalog ------------------------------------------------------------------


def test_species_added_creates_the_catalogue_row(read_side_database: str) -> None:
    """A species that entered the catalogue has no prior row to update."""
    PROJECTIONS["garden"].apply(plant_added(), consumer_group=group("garden"))

    PROJECTIONS["catalog"].apply(species_added(), consumer_group=group("catalog"))

    species = SpeciesReadModel.objects.get(species_id=SPECIES.value)
    assert species.scientific_name == "Aloe vera"
    assert species.common_name == "Aloe"
    assert species.watering_interval == WEEK
    assert species.light_requirement == "medium"
    assert species.version == 1
    assert PlantReadModel.objects.get(plant_id=PLANT.value).species_name == "Aloe"


def test_species_updated_names_the_plants_of_that_species(read_side_database: str) -> None:
    PROJECTIONS["garden"].apply(plant_added(), consumer_group=group("garden"))

    PROJECTIONS["catalog"].apply(species_updated(version=3), consumer_group=group("catalog"))

    species = SpeciesReadModel.objects.get(species_id=SPECIES.value)
    assert species.common_name == "Boston fern"
    assert species.light_requirement == "medium"
    assert species.version == 3
    assert PlantReadModel.objects.get(plant_id=PLANT.value).species_name == "Boston fern"


# --- Notifications ------------------------------------------------------------


def test_a_notification_is_projected_and_then_acknowledged(read_side_database: str) -> None:
    projection = PROJECTIONS["notifications"]
    notification = NotificationId.new()
    projection.apply(
        NotificationCreated(
            notification_id=notification,
            household_id=HOUSEHOLD,
            notification_type=NotificationType.WATERING_DUE,
            payload={"plant_id": str(PLANT)},
            created_at=NOW,
        ),
        consumer_group=group("notifications"),
    )

    row = NotificationReadModel.objects.get(notification_id=notification.value)
    assert row.notification_type == "watering_due"
    assert row.payload == {"plant_id": str(PLANT)}
    assert row.read_at is None

    projection.apply(
        NotificationRead(notification_id=notification, household_id=HOUSEHOLD, read_at=LATER),
        consumer_group=group("notifications"),
    )

    assert NotificationReadModel.objects.get(notification_id=notification.value).read_at == LATER


# --- Journal ------------------------------------------------------------------


def test_a_journal_entry_keeps_both_of_its_timestamps(read_side_database: str) -> None:
    entry = journal_entry()

    PROJECTIONS["journal"].apply(entry, consumer_group=group("journal"))

    row = JournalReadModel.objects.get(entry_id=entry.entry_id.value)
    assert row.entry_type == "watering"
    assert row.note == "Watered"
    assert row.occurred_at == NOW
    assert row.recorded_at == entry.occurred_at


def test_a_journal_entry_can_arrive_before_the_plant_event(read_side_database: str) -> None:
    """The entry's foreign key is honoured, and the garden event fills the rest."""
    entry = journal_entry()
    PROJECTIONS["journal"].apply(entry, consumer_group=group("journal"))

    placeholder = PlantReadModel.objects.get(plant_id=PLANT.value)
    assert placeholder.name is None

    PROJECTIONS["garden"].apply(plant_added(), consumer_group=group("garden"))

    assert PlantReadModel.objects.get(plant_id=PLANT.value).name == "Fern"
    assert JournalReadModel.objects.get(entry_id=entry.entry_id.value).plant_id == PLANT.value


# --- Idempotency --------------------------------------------------------------


def test_a_replayed_event_is_projected_once(read_side_database: str) -> None:
    projection = PROJECTIONS["garden"]
    event = plant_added(name="First")

    assert projection.apply(event, consumer_group=group("garden")) is True
    assert projection.apply(event, consumer_group=group("garden")) is False

    assert PlantReadModel.objects.filter(plant_id=PLANT.value).count() == 1
    assert PlantReadModel.objects.get(plant_id=PLANT.value).name == "First"
    assert ProcessedEvent.objects.filter(event_id=event.event_id).count() == 1


def test_each_consumer_group_has_its_own_idempotency_domain(read_side_database: str) -> None:
    """A ledger row is per group: another group may still have to project the event."""
    projection = PROJECTIONS["garden"]
    event = plant_added()

    assert projection.apply(event, consumer_group="read-a") is True
    assert projection.apply(event, consumer_group="read-b") is True
    assert ProcessedEvent.objects.filter(event_id=event.event_id).count() == 2


def test_an_event_a_projection_does_not_handle_claims_nothing(read_side_database: str) -> None:
    """A topic carries every event of its context; the others are not this one's."""
    due = WateringDue(plant_id=PLANT, due_at=NOW)

    assert PROJECTIONS["garden"].apply(due, consumer_group=group("garden")) is False

    assert ProcessedEvent.objects.count() == 0


class FailingProjection(Projection):
    """Writes, then fails, to prove the ledger and the write are one transaction."""

    name = "failing"
    topics = ("test.events",)

    def on_plant_added(self, event: PlantAdded) -> None:
        """Write the row and then fail before the transaction commits."""
        PlantReadModel.objects.update_or_create(
            plant_id=event.plant_id.value,
            defaults={"name": event.name},
        )
        raise RuntimeError("the projection could not finish")

    handlers: ClassVar[dict[type[DomainEvent], ProjectionHandler]] = {PlantAdded: on_plant_added}


def test_a_failing_projection_rolls_back_its_writes_and_its_claim(read_side_database: str) -> None:
    """A redelivery after a failure must start from nothing, not from a claim."""
    event = plant_added()

    with pytest.raises(RuntimeError, match="could not finish"):
        FailingProjection().apply(event, consumer_group=group("failing"))

    assert PlantReadModel.objects.count() == 0
    assert ProcessedEvent.objects.count() == 0


# --- The admin's own rendering -------------------------------------------------


def test_every_read_model_renders_itself_for_the_admin(read_side_database: str) -> None:
    """Django renders ``str(obj)`` in the changelist, in inlines and in FK widgets."""
    PROJECTIONS["garden"].apply(plant_added(), consumer_group=group("garden"))
    PROJECTIONS["care"].apply(care_schedule_created(), consumer_group=group("care"))
    PROJECTIONS["catalog"].apply(species_updated(), consumer_group=group("catalog"))
    entry = journal_entry(note="Watered")
    PROJECTIONS["journal"].apply(entry, consumer_group=group("journal"))
    notification = NotificationId.new()
    PROJECTIONS["notifications"].apply(
        NotificationCreated(
            notification_id=notification,
            household_id=HOUSEHOLD,
            notification_type=NotificationType.WATERING_DUE,
            payload={},
            created_at=NOW,
        ),
        consumer_group=group("notifications"),
    )

    assert str(PlantReadModel.objects.get(plant_id=PLANT.value)) == "Fern"
    assert str(CareReadModel.objects.get(plant_id=PLANT.value)) == f"care for {PLANT.value}"
    assert str(SpeciesReadModel.objects.get(species_id=SPECIES.value)) == "Boston fern"
    assert (
        str(JournalReadModel.objects.get(entry_id=entry.entry_id.value)) == "watering at 2026-01-01"
    )
    assert str(NotificationReadModel.objects.get(notification_id=notification.value)) == (
        f"watering_due for {HOUSEHOLD.value}"
    )

    ledger = ProcessedEvent.objects.filter(event_id=entry.event_id).get()
    assert str(ledger) == f"{group('journal')}:{entry.event_id}"


# --- The async wrapper the subscriber awaits ----------------------------------


async def test_apply_event_runs_the_projection_on_a_thread(read_side_database: str) -> None:
    """The wrapper the FastStream subscriber awaits is the one tests can drive."""
    event = plant_added()

    assert await apply_event(PROJECTIONS["garden"], event, consumer_group=group("async")) is True

    row = await PlantReadModel.objects.aget(plant_id=PLANT.value)
    assert row.name == "Fern"


async def test_apply_event_is_idempotent_across_calls(read_side_database: str) -> None:
    event = plant_added()

    assert await apply_event(PROJECTIONS["garden"], event, consumer_group=group("async")) is True
    assert await apply_event(PROJECTIONS["garden"], event, consumer_group=group("async")) is False


# --- The failure policy the subscriber runs a delivery under ------------------


@dataclass
class Plan:
    """How the test's projection fails, and how many attempts it took."""

    terminal: int = 0
    """Deliveries that break a domain rule and cannot succeed at all."""

    transient: int = 0
    """Deliveries that fail the retryable way before one succeeds."""

    attempts: int = 0
    """Every attempt taken, retries included."""

    def take_attempt(self) -> None:
        """Count this attempt, and fail it if the plan says so."""
        self.attempts += 1
        if self.terminal > 0:
            self.terminal -= 1
            raise PlantAlreadyRemovedError("this plant is gone")
        if self.transient > 0:
            self.transient -= 1
            raise RuntimeError("the read database went away")


class FlakyProjection(Projection):
    """A projection that writes its row, or fails trying."""

    name = "flaky"
    topics = (GARDEN_EVENTS,)

    def __init__(self, plan: Plan) -> None:
        self._plan = plan

    def on_plant_added(self, event: PlantAdded) -> None:
        """Write the row, unless this attempt is a planned failure."""
        self._plan.take_attempt()
        PlantReadModel.objects.update_or_create(
            plant_id=event.plant_id.value,
            defaults={"name": event.name},
        )

    handlers: ClassVar[dict[type[DomainEvent], ProjectionHandler]] = {PlantAdded: on_plant_added}


class RecordingDeadLetterPublisher:
    """A dead-letter publisher that keeps what it was asked to copy."""

    def __init__(self, *, refuse: bool = False) -> None:
        self.refuse = refuse
        self.copies: list[DeliveryDeadLetter] = []

    async def publish_moved_aside(self, delivery: DeliveryDeadLetter) -> None:
        """Record the copy, or fail like a broker that will not take it."""
        if self.refuse:
            raise RuntimeError("dead-letter topic unavailable")
        self.copies.append(delivery)


def a_message_for(event: DomainEvent) -> KafkaMessage:
    """Build the delivery the read side's subscriber receives for ``event``."""
    body = event.model_dump_json().encode("utf-8")
    record = ConsumerRecord(
        topic=GARDEN_EVENTS,
        partition=0,
        offset=0,
        timestamp=0,
        timestamp_type=0,
        key=str(event.event_id).encode("utf-8"),
        value=body,
        headers=[],
        checksum=None,
        serialized_key_size=-1,
        serialized_value_size=-1,
    )
    return KafkaMessage(
        cast("Any", record),
        body,
        headers={HEADER_EVENT_NAME: type(event).__name__, HEADER_EVENT_ID: str(event.event_id)},
        consumer=cast("ConsumerProtocol", None),
    )


def a_handler(
    projection: Projection, publisher: DeadLetterPublisher, *, attempts: int = 3
) -> ProjectionSubscriber:
    """Build the production subscriber handler over the test's doubles."""
    return build_handler(
        projection,
        consumer_group=group("flaky"),
        policy=FailurePolicy(max_attempts=attempts, initial_wait_seconds=0.0, max_wait_seconds=0.0),
        publisher=publisher,
    )


async def test_a_broken_domain_rule_is_copied_aside_and_claimed(read_side_database: str) -> None:
    """The read side's half of the policy: one attempt, one copy, one ledger row."""
    plan = Plan(terminal=1)
    publisher = RecordingDeadLetterPublisher()
    event = plant_added()

    await a_handler(FlakyProjection(plan), publisher)(a_message_for(event))

    assert plan.attempts == 1
    assert len(publisher.copies) == 1
    copy = publisher.copies[0]
    assert copy.consumer_group == group("flaky")
    assert copy.original_topic == GARDEN_EVENTS
    assert copy.headers[HEADER_EVENT_ID] == str(event.event_id)
    assert copy.error.startswith("PlantAlreadyRemovedError")
    assert await PlantReadModel.objects.acount() == 0
    assert await ProcessedEvent.objects.filter(
        consumer_group=group("flaky"), event_id=event.event_id
    ).aexists()


async def test_a_transient_failure_is_retried_until_the_projection_applies(
    read_side_database: str,
) -> None:
    """The failed attempt's write rolled back with its claim; the retry starts over."""
    plan = Plan(transient=1)
    publisher = RecordingDeadLetterPublisher()
    event = plant_added()

    await a_handler(FlakyProjection(plan), publisher)(a_message_for(event))

    assert plan.attempts == 2
    assert publisher.copies == []
    assert await PlantReadModel.objects.acount() == 1
    assert (await PlantReadModel.objects.aget(plant_id=PLANT.value)).name == "Fern"
    assert await ProcessedEvent.objects.filter(event_id=event.event_id).acount() == 1


async def test_a_refused_copy_leaves_the_projection_unclaimed(read_side_database: str) -> None:
    """Nothing was projected and nothing was stored: the broker must offer it again."""
    plan = Plan(terminal=1)
    publisher = RecordingDeadLetterPublisher(refuse=True)
    event = plant_added()

    with pytest.raises(RuntimeError, match="dead-letter topic unavailable"):
        await a_handler(FlakyProjection(plan), publisher)(a_message_for(event))

    assert await ProcessedEvent.objects.acount() == 0
    assert await PlantReadModel.objects.acount() == 0

    publisher.refuse = False
    await a_handler(FlakyProjection(plan), publisher)(a_message_for(event))
    assert await PlantReadModel.objects.acount() == 1


async def test_the_next_event_is_projected_after_one_is_moved_aside(
    read_side_database: str,
) -> None:
    """A delivery moved aside must not stop the projection from taking the next one."""
    plan = Plan(terminal=1)
    publisher = RecordingDeadLetterPublisher()
    handler = a_handler(FlakyProjection(plan), publisher)
    poison = plant_added(name="Fern")
    following = PlantAdded(
        plant_id=PlantId.new(),
        household_id=HOUSEHOLD,
        species_id=SPECIES,
        name="Aloe",
        location=Location(value="Shelf"),
        added_at=NOW,
    )

    await handler(a_message_for(poison))
    await handler(a_message_for(following))

    assert len(publisher.copies) == 1
    assert publisher.copies[0].headers[HEADER_EVENT_ID] == str(poison.event_id)
    assert await PlantReadModel.objects.acount() == 1
    assert await PlantReadModel.objects.filter(plant_id=following.plant_id.value).aexists()
    assert await ProcessedEvent.objects.acount() == 2


# --- Telemetry ------------------------------------------------------------------


SENSOR = SensorId.new()
"""The sensor the telemetry tests project readings for."""

ROLLUP_FIELDS = (
    "sensor_id",
    "bucket_start",
    "plant_id",
    "moisture_min",
    "moisture_max",
    "moisture_avg",
    "temperature_min",
    "temperature_max",
    "temperature_avg",
    "sample_count",
)
"""A window's own columns: the surrogate id and the write timestamp are not the answer."""


def a_reading(
    *,
    recorded_at: datetime = NOW,
    sensor_id: SensorId = SENSOR,
    moisture: float = 42.5,
    temperature: float = 21.0,
    light: float = 800.0,
) -> TelemetryReceived:
    """One ``TelemetryReceived``, as the ingress publishes it."""
    return TelemetryReceived(
        sensor_id=sensor_id,
        plant_id=PLANT,
        recorded_at=recorded_at,
        moisture=Moisture(value=moisture),
        temperature=Temperature(value=temperature),
        light=LightLevel(value=light),
    )


def an_alert(
    event_type: type[DomainEvent],
    *,
    recorded_at: datetime = NOW,
    sensor_id: SensorId = SENSOR,
) -> DomainEvent:
    """One threshold alert, as ``Sensor.record`` raises it."""
    if event_type is SoilMoistureLow:
        return SoilMoistureLow(
            sensor_id=sensor_id,
            plant_id=PLANT,
            moisture=Moisture(value=12.0),
            threshold=30.0,
            occurred_at=recorded_at,
        )
    if event_type is SoilMoistureHigh:
        return SoilMoistureHigh(
            sensor_id=sensor_id,
            plant_id=PLANT,
            moisture=Moisture(value=92.0),
            threshold=80.0,
            occurred_at=recorded_at,
        )
    assert event_type is TemperatureAnomaly
    return TemperatureAnomaly(
        sensor_id=sensor_id,
        plant_id=PLANT,
        temperature=Temperature(value=41.0),
        low_threshold=10.0,
        high_threshold=35.0,
        occurred_at=recorded_at,
    )


def a_silence(
    *, occurred_at: datetime = NOW + timedelta(minutes=30), sensor_id: SensorId = SENSOR
) -> SensorOffline:
    """One ``SensorOffline``, as the silence timer publishes it."""
    return SensorOffline(
        sensor_id=sensor_id,
        plant_id=PLANT,
        last_seen_at=NOW,
        offline_for=occurred_at - NOW,
        occurred_at=occurred_at,
    )


def project_telemetry(event: DomainEvent) -> bool:
    """Apply one telemetry event the way the consumer does."""
    return PROJECTIONS["telemetry-rollup"].apply(event, consumer_group=group("telemetry-rollup"))


def test_a_reading_creates_its_window_and_the_sensor_latest_row(read_side_database: str) -> None:
    reading = a_reading(moisture=42.5, temperature=21.0, light=800.0)

    assert project_telemetry(reading) is True

    rollup = TelemetryRollup.objects.get(sensor_id=SENSOR.value, bucket_start=NOW)
    assert rollup.plant_id == PLANT.value
    assert rollup.moisture_min == 42.5
    assert rollup.moisture_max == 42.5
    assert rollup.moisture_avg == 42.5
    assert rollup.sample_count == 1

    latest = SensorLatest.objects.get(sensor_id=SENSOR.value)
    assert latest.plant_id == PLANT.value
    assert latest.recorded_at == NOW
    assert latest.moisture == 42.5
    assert latest.temperature == 21.0
    assert latest.light == 800.0
    assert latest.offline_at is None
    assert latest.last_alert_at is None


def test_a_second_reading_in_the_window_updates_the_same_rollup(read_side_database: str) -> None:
    """A window is updated, never duplicated, and its aggregate stays exact."""
    project_telemetry(a_reading(moisture=40.0, recorded_at=NOW + timedelta(minutes=5)))
    project_telemetry(a_reading(moisture=10.0, recorded_at=NOW + timedelta(minutes=45)))

    assert TelemetryRollup.objects.count() == 1
    rollup = TelemetryRollup.objects.get()
    assert rollup.sample_count == 2
    assert rollup.moisture_min == 10.0
    assert rollup.moisture_max == 40.0
    assert rollup.moisture_avg == pytest.approx(25.0)


def test_an_out_of_order_reading_recomputes_its_window(read_side_database: str) -> None:
    """The window is revisited, not appended to: min, max, mean and count all hold."""
    project_telemetry(a_reading(moisture=40.0, temperature=20.0, recorded_at=NOW))
    project_telemetry(
        a_reading(moisture=10.0, temperature=30.0, recorded_at=NOW + timedelta(minutes=45))
    )
    project_telemetry(
        a_reading(moisture=25.0, temperature=25.0, recorded_at=NOW + timedelta(minutes=15))
    )

    assert TelemetryRollup.objects.count() == 1
    rollup = TelemetryRollup.objects.get()
    assert rollup.sample_count == 3
    assert rollup.moisture_min == 10.0
    assert rollup.moisture_max == 40.0
    assert rollup.moisture_avg == pytest.approx(25.0)
    assert rollup.temperature_min == 20.0
    assert rollup.temperature_max == 30.0
    assert rollup.temperature_avg == pytest.approx(25.0)


def test_readings_in_different_windows_get_their_own_rows(read_side_database: str) -> None:
    project_telemetry(a_reading(recorded_at=NOW))
    project_telemetry(a_reading(recorded_at=NOW + timedelta(hours=1)))

    assert TelemetryRollup.objects.count() == 2
    assert TelemetryRollup.objects.filter(bucket_start=NOW).count() == 1
    assert TelemetryRollup.objects.filter(bucket_start=NOW + timedelta(hours=1)).count() == 1


def test_an_older_reading_does_not_move_the_latest_row_backwards(
    read_side_database: str,
) -> None:
    project_telemetry(a_reading(moisture=50.0, recorded_at=NOW + timedelta(minutes=30)))
    project_telemetry(a_reading(moisture=5.0, recorded_at=NOW))

    latest = SensorLatest.objects.get(sensor_id=SENSOR.value)
    assert latest.recorded_at == NOW + timedelta(minutes=30)
    assert latest.moisture == 50.0
    # The window still records it: only "latest" refuses to go backwards.
    assert TelemetryRollup.objects.get().sample_count == 2


@pytest.mark.parametrize(
    ("event_type", "expected"),
    [
        (SoilMoistureLow, "soil_moisture_low"),
        (SoilMoistureHigh, "soil_moisture_high"),
        (TemperatureAnomaly, "temperature_anomaly"),
    ],
    ids=["low", "high", "anomaly"],
)
def test_every_alert_event_is_recorded_on_the_sensor_latest_row(
    event_type: type[DomainEvent], expected: str, read_side_database: str
) -> None:
    """The alerts are projected, not listed as unprojected: the row says which band."""
    project_telemetry(a_reading())

    project_telemetry(an_alert(event_type, recorded_at=NOW + timedelta(minutes=1)))

    latest = SensorLatest.objects.get(sensor_id=SENSOR.value)
    assert latest.last_alert_type == expected
    assert latest.last_alert_at == NOW + timedelta(minutes=1)
    # And the alert is not a second sample in the window.
    assert TelemetryRollup.objects.get().sample_count == 1


def test_an_older_alert_does_not_replace_a_newer_one(read_side_database: str) -> None:
    project_telemetry(a_reading())
    project_telemetry(an_alert(TemperatureAnomaly, recorded_at=NOW + timedelta(minutes=10)))
    project_telemetry(an_alert(SoilMoistureLow, recorded_at=NOW + timedelta(minutes=5)))

    latest = SensorLatest.objects.get(sensor_id=SENSOR.value)
    assert latest.last_alert_type == "temperature_anomaly"
    assert latest.last_alert_at == NOW + timedelta(minutes=10)


def test_a_silence_is_recorded_and_a_later_reading_clears_it(read_side_database: str) -> None:
    project_telemetry(a_reading())
    project_telemetry(a_silence())

    assert SensorLatest.objects.get(sensor_id=SENSOR.value).offline_at == NOW + timedelta(
        minutes=30
    )

    project_telemetry(a_reading(recorded_at=NOW + timedelta(minutes=40)))

    assert SensorLatest.objects.get(sensor_id=SENSOR.value).offline_at is None


def test_the_read_side_reproduces_the_rollups_by_replaying_the_topic(
    read_side_database: str,
) -> None:
    """Rebuild: drop the rollups and the group's ledger, replay, get the same rows."""
    consumer_group = group("telemetry-rollup")
    events: list[DomainEvent] = [
        a_reading(moisture=40.0, recorded_at=NOW),
        a_reading(moisture=10.0, recorded_at=NOW + timedelta(minutes=30)),
        an_alert(SoilMoistureLow, recorded_at=NOW + timedelta(minutes=30)),
        a_reading(moisture=60.0, recorded_at=NOW + timedelta(hours=1)),
        a_silence(occurred_at=NOW + timedelta(hours=2)),
    ]
    for event in events:
        project_telemetry(event)

    before_rollups = list(TelemetryRollup.objects.order_by("bucket_start").values(*ROLLUP_FIELDS))
    before_latest = SensorLatest.objects.get(sensor_id=SENSOR.value)
    assert len(before_rollups) == 2

    # The rebuild procedure: the read tables and the group's ledger rows go, the
    # topic and its offsets stay, and the consumer replays from the beginning.
    TelemetryRollup.objects.all().delete()
    SensorLatest.objects.all().delete()
    ProcessedEvent.objects.filter(consumer_group=consumer_group).delete()

    for event in events:
        assert project_telemetry(event) is True

    assert (
        list(TelemetryRollup.objects.order_by("bucket_start").values(*ROLLUP_FIELDS))
        == before_rollups
    )
    latest = SensorLatest.objects.get(sensor_id=SENSOR.value)
    assert latest.recorded_at == before_latest.recorded_at
    assert latest.offline_at == before_latest.offline_at
    assert latest.last_alert_type == before_latest.last_alert_type


def test_a_replayed_telemetry_event_is_projected_once(read_side_database: str) -> None:
    """The group's own ledger is what keeps a redelivery from double-counting."""
    reading = a_reading()

    assert project_telemetry(reading) is True
    assert project_telemetry(reading) is False

    assert TelemetryRollup.objects.get().sample_count == 1
    assert ProcessedEvent.objects.filter(event_id=reading.event_id).count() == 1
