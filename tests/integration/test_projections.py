"""Integration tests for the read side's projections.

They run against the migrated ``read_analytics`` schema in the testcontainers
Postgres, and they call the projections the way the consumer does:
``Projection.apply`` inside a transaction, with the ledger in the same
transaction. Django's ORM is synchronous, so the tests are too — except the two
that prove the async wrapper the FastStream subscriber uses.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import ClassVar

import pytest

from plantkeeper.admin.projections import ALL_PROJECTIONS
from plantkeeper.admin.projections.base import Projection, ProjectionHandler, apply_event
from plantkeeper.admin.read_models.models import (
    CareReadModel,
    JournalReadModel,
    NotificationReadModel,
    PlantReadModel,
    ProcessedEvent,
    SpeciesReadModel,
)
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
    SpeciesCacheInvalidated,
    SpeciesSyncRequested,
    SpeciesUpdated,
)
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.garden.events import PlantAdded, PlantMoved, PlantOnboarded, PlantRemoved
from plantkeeper.domain.identifiers import (
    HouseholdId,
    JournalEntryId,
    NotificationId,
    PlantId,
    SpeciesId,
)
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.journal.values import JournalEntryType
from plantkeeper.domain.notifications.events import NotificationCreated, NotificationRead
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.domain.telemetry.events import (
    SensorOffline,
    SoilMoistureHigh,
    SoilMoistureLow,
    TelemetryReceived,
    TemperatureAnomaly,
)
from plantkeeper.domain.values import Location, WateringInterval
from plantkeeper.infrastructure.messaging.topics import EVENT_TOPICS

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
    # Telemetry has no read model before Phase 5.
    TelemetryReceived,
    SoilMoistureLow,
    SoilMoistureHigh,
    TemperatureAnomaly,
    SensorOffline,
}
"""Catalogue events no Phase 3 projection consumes, named so the gap is a decision."""


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
