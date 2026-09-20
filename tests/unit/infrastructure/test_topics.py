"""Contract test for the Kafka topic registry.

``docs/events.md`` fixes the event catalogue, ``tests/unit/domain/test_events_catalogue.py``
guards it, and this module guards the other half of the contract: every one of
those 25 events must have a topic, and no topic may be invented for an event that
does not exist.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid7

import pytest

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
    CARE_EVENTS,
    CATALOG_EVENTS,
    EVENT_TOPICS,
    EVENT_TYPES,
    GARDEN_EVENTS,
    JOURNAL_EVENTS,
    NOTIFICATIONS_EVENTS,
    SAGA_EVENTS,
    TELEMETRY_EVENTS,
    UnmappedEventError,
    event_type_for,
    partition_key_for,
    topic_for,
)

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
WEEK = WateringInterval(value=timedelta(days=7))
PLANT = PlantId.new()
HOUSEHOLD = HouseholdId.new()
SPECIES = SpeciesId.new()
SENSOR = SensorId.new()
SAGA = uuid7()

EVENT_SAMPLES: list[DomainEvent] = [
    PlantAdded(
        plant_id=PLANT,
        household_id=HOUSEHOLD,
        species_id=SPECIES,
        name="Fern",
        location=Location(value="Shelf"),
        added_at=NOW,
        occurred_at=NOW,
    ),
    PlantRemoved(plant_id=PLANT, removed_at=NOW, occurred_at=NOW),
    PlantMoved(
        plant_id=PLANT,
        previous_location=Location(value="Shelf"),
        location=Location(value="Window"),
        occurred_at=NOW,
    ),
    PlantOnboarded(
        plant_id=PLANT, household_id=HOUSEHOLD, species_id=SPECIES, next_watering_at=NOW
    ),
    CareScheduleCreated(plant_id=PLANT, watering_interval=WEEK, next_watering_at=NOW),
    WateringDue(plant_id=PLANT, due_at=NOW),
    WateringCompleted(plant_id=PLANT, completed_at=NOW, next_watering_at=NOW),
    WateringRescheduled(
        plant_id=PLANT, previous_next_watering_at=NOW, next_watering_at=NOW, reason=None
    ),
    CareMissed(plant_id=PLANT, next_watering_at=NOW),
    CareSkipped(plant_id=PLANT, skipped_at=NOW, next_watering_at=NOW),
    SpeciesSyncRequested(),
    SpeciesUpdated(
        species_id=SPECIES,
        scientific_name="Nephrolepis exaltata",
        common_name="Boston fern",
        watering_interval=WEEK,
        light_requirement=LightRequirement.MEDIUM,
        version=2,
    ),
    SpeciesCacheInvalidated(species_id=SPECIES),
    JournalEntryAdded(
        entry_id=JournalEntryId.new(),
        plant_id=PLANT,
        entry_type=JournalEntryType.WATERING,
        note=None,
        entry_occurred_at=NOW,
    ),
    TelemetryReceived(
        sensor_id=SENSOR,
        plant_id=PLANT,
        recorded_at=NOW,
        moisture=Moisture(value=42.0),
        temperature=Temperature(value=21.0),
        light=LightLevel(value=800.0),
    ),
    SoilMoistureLow(
        sensor_id=SENSOR, plant_id=PLANT, moisture=Moisture(value=12.0), threshold=30.0
    ),
    SoilMoistureHigh(
        sensor_id=SENSOR, plant_id=PLANT, moisture=Moisture(value=95.0), threshold=80.0
    ),
    TemperatureAnomaly(
        sensor_id=SENSOR,
        plant_id=PLANT,
        temperature=Temperature(value=40.0),
        low_threshold=10.0,
        high_threshold=35.0,
    ),
    SensorOffline(
        sensor_id=SENSOR, plant_id=PLANT, last_seen_at=NOW, offline_for=timedelta(minutes=15)
    ),
    NotificationCreated(
        notification_id=NotificationId.new(),
        household_id=HOUSEHOLD,
        notification_type=NotificationType.WATERING_DUE,
        payload={"plant_id": str(PLANT)},
        created_at=NOW,
    ),
    NotificationRead(notification_id=NotificationId.new(), household_id=HOUSEHOLD, read_at=NOW),
    SagaStarted(saga_id=SAGA, saga_name="OnboardPlantSaga", occurred_at=NOW),
    SagaCompleted(saga_id=SAGA, saga_name="OnboardPlantSaga", occurred_at=NOW),
    SagaFailed(saga_id=SAGA, saga_name="OnboardPlantSaga", error="boom", occurred_at=NOW),
    SagaCompensated(saga_id=SAGA, saga_name="OnboardPlantSaga", occurred_at=NOW),
]


def test_every_catalogued_event_has_a_topic() -> None:
    assert len(EVENT_SAMPLES) == 25
    assert {type(event) for event in EVENT_SAMPLES} == set(EVENT_TOPICS)


@pytest.mark.parametrize("event", EVENT_SAMPLES, ids=lambda event: type(event).__name__)
def test_topic_for_returns_the_mapped_topic(event: DomainEvent) -> None:
    assert topic_for(event) == EVENT_TOPICS[type(event)]


def test_topics_are_grouped_per_bounded_context() -> None:
    assert {topic_for(event) for event in EVENT_SAMPLES} == {
        GARDEN_EVENTS,
        CARE_EVENTS,
        CATALOG_EVENTS,
        JOURNAL_EVENTS,
        TELEMETRY_EVENTS,
        NOTIFICATIONS_EVENTS,
        SAGA_EVENTS,
    }


def test_an_unmapped_event_type_is_refused() -> None:
    class Invented(DomainEvent):
        """An event that nobody mapped to a topic."""

    with pytest.raises(UnmappedEventError, match="Invented"):
        topic_for(Invented())


@pytest.mark.parametrize("event", EVENT_SAMPLES, ids=lambda event: type(event).__name__)
def test_every_event_has_a_partition_key(event: DomainEvent) -> None:
    assert partition_key_for(event)


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        (
            PlantAdded(
                plant_id=PLANT,
                household_id=HOUSEHOLD,
                species_id=SPECIES,
                name="Fern",
                location=Location(value="Shelf"),
                added_at=NOW,
            ),
            str(PLANT),
        ),
        (
            NotificationRead(
                notification_id=NotificationId.new(), household_id=HOUSEHOLD, read_at=NOW
            ),
            str(HOUSEHOLD),
        ),
        (SpeciesCacheInvalidated(species_id=SPECIES), str(SPECIES)),
        (WateringDue(plant_id=PLANT, due_at=NOW), str(PLANT)),
        # A saga lifecycle event carries no aggregate id: its saga keys the partition.
        (SagaCompleted(saga_id=SAGA, saga_name="OnboardPlantSaga"), str(SAGA)),
    ],
)
def test_partition_key_prefers_an_aggregate_identifier(event: DomainEvent, expected: str) -> None:
    assert partition_key_for(event) == expected


def test_partition_key_falls_back_to_the_event_id() -> None:
    event = SpeciesSyncRequested()
    assert partition_key_for(event) == str(event.event_id)


def test_the_event_type_registry_covers_the_catalogue() -> None:
    """A consumer resolves ``event_name`` back to a model; it must resolve all of them."""
    assert set(EVENT_TYPES) == {type(event).__name__ for event in EVENT_SAMPLES}
    assert len(EVENT_TYPES) == len(EVENT_SAMPLES), "an event name maps to one model only"


@pytest.mark.parametrize("event", EVENT_SAMPLES, ids=lambda event: type(event).__name__)
def test_event_type_for_round_trips_the_header(event: DomainEvent) -> None:
    assert event_type_for(type(event).__name__) is type(event)


def test_an_unknown_event_name_has_no_type() -> None:
    """Unknown is ``None``, not an error: only the consumer can decide to skip it."""
    assert event_type_for("Invented") is None
