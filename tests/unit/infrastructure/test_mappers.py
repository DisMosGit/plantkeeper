"""Unit tests for the aggregate <-> row mappers.

Two properties are worth pinning, and both are cheap to check without a
database: a domain object survives a round trip unchanged, and the enum-valued
columns store their *value* rather than the member name.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pydantic import JsonValue

from plantkeeper.application.ports.idempotency import IdempotencyRecord
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.garden.events import PlantAdded
from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import (
    HouseholdId,
    JournalEntryId,
    NotificationId,
    PlantId,
    SensorId,
    SpeciesId,
)
from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.domain.journal.values import JournalEntryType
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.domain.telemetry.sensor import Sensor
from plantkeeper.domain.values import Location, WateringInterval
from plantkeeper.infrastructure.messaging.topics import GARDEN_EVENTS
from plantkeeper.infrastructure.persistence.mappers import (
    care_schedule_to_domain,
    care_schedule_to_model,
    household_to_domain,
    household_to_model,
    idempotency_to_domain,
    idempotency_to_model,
    journal_entry_to_domain,
    journal_entry_to_model,
    notification_to_domain,
    notification_to_model,
    outbox_message_from_model,
    outbox_model_from_event,
    plant_to_domain,
    plant_to_model,
    sensor_to_domain,
    sensor_to_model,
    species_to_domain,
    species_to_model,
)

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
LATER = NOW + timedelta(hours=3)


def test_a_plant_survives_a_round_trip() -> None:
    plant = Plant(
        PlantId.new(),
        household_id=HouseholdId.new(),
        species_id=SpeciesId.new(),
        name="Fern",
        location=Location(value="Shelf"),
        added_at=NOW,
        last_watered_at=LATER,
        last_repotted_at=None,
        removed=False,
    )

    restored = plant_to_domain(plant_to_model(plant))

    assert restored.id == plant.id
    assert restored.household_id == plant.household_id
    assert restored.species_id == plant.species_id
    assert restored.name == plant.name
    assert restored.location == plant.location
    assert restored.added_at == plant.added_at
    assert restored.last_watered_at == LATER
    assert restored.last_repotted_at is None
    assert restored.is_removed is False


def test_a_removed_plant_keeps_its_flag() -> None:
    plant = Plant(
        PlantId.new(),
        household_id=HouseholdId.new(),
        species_id=SpeciesId.new(),
        name="Fern",
        location=Location(value="Shelf"),
        added_at=NOW,
        removed=True,
    )
    assert plant_to_domain(plant_to_model(plant)).is_removed is True


def test_a_household_takes_its_membership_from_the_caller() -> None:
    household = Household(HouseholdId.new(), name="Home")
    plant_ids = [PlantId.new(), PlantId.new()]

    restored = household_to_domain(household_to_model(household), plant_ids)

    assert restored.name == "Home"
    assert restored.plant_count == 2
    assert restored.id == household.id


def test_a_care_schedule_survives_a_round_trip() -> None:
    schedule = CareSchedule(
        PlantId.new(),
        watering_interval=WateringInterval(value=timedelta(days=7)),
        next_watering_at=LATER,
        version=3,
    )

    restored = care_schedule_to_domain(care_schedule_to_model(schedule))

    assert restored.id == schedule.id
    assert restored.watering_interval == schedule.watering_interval
    assert restored.next_watering_at == LATER
    assert restored.version == 3


def test_a_species_stores_its_light_requirement_by_value() -> None:
    species = Species(
        SpeciesId.new(),
        scientific_name="Nephrolepis exaltata",
        common_name="Boston fern",
        watering_interval=WateringInterval(value=timedelta(days=7)),
        light_requirement=LightRequirement.MEDIUM,
        version=2,
    )

    model = species_to_model(species)

    assert model.light_requirement == "medium"
    assert species_to_domain(model) == species
    assert species_to_domain(model).version == 2


def test_a_journal_entry_survives_a_round_trip() -> None:
    entry = JournalEntry(
        JournalEntryId.new(),
        plant_id=PlantId.new(),
        entry_type=JournalEntryType.WATERING,
        occurred_at=NOW,
        note="watered thoroughly",
    )

    restored = journal_entry_to_domain(journal_entry_to_model(entry))

    assert restored.id == entry.id
    assert restored.plant_id == entry.plant_id
    assert restored.entry_type is JournalEntryType.WATERING
    assert restored.note == "watered thoroughly"


def test_a_journal_entry_note_may_be_absent() -> None:
    entry = JournalEntry(
        JournalEntryId.new(),
        plant_id=PlantId.new(),
        entry_type=JournalEntryType.NOTE,
        occurred_at=NOW,
    )
    assert journal_entry_to_domain(journal_entry_to_model(entry)).note is None


def test_a_sensor_survives_a_round_trip() -> None:
    sensor = Sensor(SensorId.new(), plant_id=PlantId.new(), added_at=NOW, last_seen_at=LATER)

    restored = sensor_to_domain(sensor_to_model(sensor))

    assert restored.id == sensor.id
    assert restored.plant_id == sensor.plant_id
    assert restored.added_at == NOW
    assert restored.last_seen_at == LATER


def test_a_notification_survives_a_round_trip() -> None:
    payload: dict[str, JsonValue] = {"plant_id": str(PlantId.new()), "moisture": 12.5}
    notification = Notification(
        NotificationId.new(),
        household_id=HouseholdId.new(),
        notification_type=NotificationType.SOIL_MOISTURE_LOW,
        created_at=NOW,
        payload=payload,
    )

    restored = notification_to_domain(notification_to_model(notification))

    assert restored.id == notification.id
    assert restored.notification_type is NotificationType.SOIL_MOISTURE_LOW
    assert restored.payload == payload
    assert restored.read_at is None


def test_an_event_becomes_an_outbox_row_with_its_topic_and_key() -> None:
    plant_id = PlantId.new()
    event = PlantAdded(
        plant_id=plant_id,
        household_id=HouseholdId.new(),
        species_id=SpeciesId.new(),
        name="Fern",
        location=Location(value="Shelf"),
        added_at=NOW,
        occurred_at=NOW,
    )

    model = outbox_model_from_event(event)

    assert model.event_id == event.event_id
    assert model.event_name == "PlantAdded"
    assert model.topic == GARDEN_EVENTS
    assert model.partition_key == str(plant_id)
    assert model.occurred_at == NOW
    assert model.attempts == 0
    assert model.payload["event_id"] == str(event.event_id)


def test_an_outbox_row_becomes_a_publishable_message() -> None:
    event: DomainEvent = PlantAdded(
        plant_id=PlantId.new(),
        household_id=HouseholdId.new(),
        species_id=SpeciesId.new(),
        name="Fern",
        location=Location(value="Shelf"),
        added_at=NOW,
        occurred_at=NOW,
    )
    model = outbox_model_from_event(event)
    model.id = 42
    model.attempts = 2

    message = outbox_message_from_model(model)

    assert message.outbox_id == 42
    assert message.event_name == "PlantAdded"
    assert message.topic == GARDEN_EVENTS
    assert message.attempts == 2
    assert message.payload == model.payload


def test_an_idempotency_record_survives_a_round_trip() -> None:
    record = IdempotencyRecord(
        key="retry-1",
        request_hash="a" * 64,
        status_code=201,
        response={"plant_id": str(PlantId.new())},
        created_at=NOW,
    )

    restored = idempotency_to_domain(idempotency_to_model(record))

    assert restored == record
