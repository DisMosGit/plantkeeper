"""Contract tests for the whole domain event catalogue.

Phase 1 fixes the catalogue in ``docs/events.md``; these tests make sure the code
and the document cannot drift apart silently.
"""

from __future__ import annotations

from datetime import UTC, datetime

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
from plantkeeper.domain.garden.events import (
    PlantAdded,
    PlantMoved,
    PlantOnboarded,
    PlantRemoved,
)
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SensorId, SpeciesId
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.notifications.events import NotificationCreated, NotificationRead
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
from plantkeeper.domain.values import LightLevel, Location, Moisture, Temperature

EVENT_TYPES: list[type[DomainEvent]] = [
    PlantAdded,
    PlantRemoved,
    PlantMoved,
    PlantOnboarded,
    CareScheduleCreated,
    WateringDue,
    WateringCompleted,
    WateringRescheduled,
    CareMissed,
    CareSkipped,
    SpeciesSyncRequested,
    SpeciesUpdated,
    SpeciesCacheInvalidated,
    JournalEntryAdded,
    TelemetryReceived,
    SoilMoistureLow,
    SoilMoistureHigh,
    TemperatureAnomaly,
    SensorOffline,
    NotificationCreated,
    NotificationRead,
    SagaStarted,
    SagaCompleted,
    SagaFailed,
    SagaCompensated,
]

EXPECTED_EVENT_NAMES = {event_type.__name__ for event_type in EVENT_TYPES}


def test_the_catalogue_has_the_documented_size() -> None:
    assert len(EVENT_TYPES) == 25
    assert len(EXPECTED_EVENT_NAMES) == 25


@pytest.mark.parametrize("event_type", EVENT_TYPES, ids=lambda event_type: event_type.__name__)
def test_every_event_carries_the_base_fields(event_type: type[DomainEvent]) -> None:
    assert issubclass(event_type, DomainEvent)
    assert {"event_id", "occurred_at"} <= set(event_type.model_fields)
    assert event_type.model_config["frozen"] is True
    assert event_type.model_config["extra"] == "forbid"


def test_a_representative_event_survives_a_json_round_trip() -> None:
    recorded_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    event = TelemetryReceived(
        sensor_id=SensorId.new(),
        plant_id=PlantId.new(),
        recorded_at=recorded_at,
        moisture=Moisture(value=42.0),
        temperature=Temperature(value=21.5),
        light=LightLevel(value=800.0),
        occurred_at=recorded_at,
    )

    assert TelemetryReceived.model_validate_json(event.model_dump_json()) == event


def test_value_objects_reach_the_wire_as_scalars() -> None:
    """The published JSON is flat: a value object is its scalar, not its wrapper.

    Consumers in any language read ``payload["location"]``; making them unwrap
    ``{"value": ...}`` would put a domain modelling detail into the integration
    contract. ``docs/events.md`` documents this shape.
    """
    added_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    event = PlantAdded(
        plant_id=PlantId.new(),
        household_id=HouseholdId.new(),
        species_id=SpeciesId.new(),
        name="Fern",
        location=Location(value="Shelf"),
        added_at=added_at,
    )

    payload = event.model_dump(mode="json")

    assert payload["location"] == "Shelf"
    assert payload["plant_id"] == str(event.plant_id)
    assert payload["added_at"] == added_at.isoformat().replace("+00:00", "Z")
