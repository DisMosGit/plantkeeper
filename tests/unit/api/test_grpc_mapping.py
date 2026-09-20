"""View → wire conversions of the gRPC mapping module."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from plantkeeper.api.grpc.generated.plantkeeper.v1 import common_pb2
from plantkeeper.api.grpc.mapping import (
    care_schedule_to_proto,
    duration_to_proto,
    household_id_from_proto,
    household_id_to_proto,
    plant_id_from_proto,
    plant_id_to_proto,
    plant_to_proto,
    sensor_id_to_proto,
    timestamp_to_proto,
)
from plantkeeper.application.views import CareScheduleView, PlantView
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SensorId, SpeciesId

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
WEEK = timedelta(days=7)


def test_identifiers_round_trip() -> None:
    plant_id = PlantId.new()
    household_id = HouseholdId.new()
    assert plant_id_to_proto(plant_id).value == str(plant_id)
    assert plant_id_from_proto(plant_id_to_proto(plant_id)) == plant_id
    assert household_id_from_proto(household_id_to_proto(household_id)) == household_id


def test_a_non_uuid_identifier_is_rejected() -> None:
    with pytest.raises(ValidationError):
        plant_id_from_proto(common_pb2.PlantId(value="not-a-uuid"))


def test_the_shared_sensor_identifier_is_wrapped() -> None:
    """``SensorId`` has no RPC yet; the mapping stays symmetric for it."""
    sensor_id = SensorId.new()
    assert sensor_id_to_proto(sensor_id).value == str(sensor_id)


def test_a_timestamp_keeps_its_instant() -> None:
    assert timestamp_to_proto(NOW).ToDatetime(tzinfo=UTC) == NOW


def test_a_duration_keeps_its_length() -> None:
    assert duration_to_proto(WEEK).ToTimedelta() == WEEK


def test_a_care_schedule_is_projected() -> None:
    view = CareScheduleView(
        plant_id=PlantId.new(),
        watering_interval=WEEK,
        next_watering_at=NOW,
        version=3,
    )
    schedule = care_schedule_to_proto(view)
    assert schedule.plant_id.value == str(view.plant_id)
    assert schedule.next_watering_at.ToDatetime(tzinfo=UTC) == NOW
    assert schedule.watering_interval.ToTimedelta() == WEEK
    assert schedule.version == 3


def an_unwatered_plant() -> PlantView:
    """A plant that was added but never watered."""
    return PlantView(
        plant_id=PlantId.new(),
        household_id=HouseholdId.new(),
        species_id=SpeciesId.new(),
        name="Fern",
        location="Shelf",
        added_at=NOW,
        last_watered_at=None,
        removed=False,
    )


def test_a_never_watered_plant_leaves_the_optional_field_unset() -> None:
    """Absent means absent: no epoch sentinel a client would have to special-case."""
    plant = plant_to_proto(an_unwatered_plant())
    assert not plant.HasField("last_watered_at")
    assert plant.name == "Fern"
    assert plant.location == "Shelf"


def test_a_watered_plant_carries_the_optional_field() -> None:
    view = an_unwatered_plant().model_copy(update={"last_watered_at": NOW, "removed": True})
    plant = plant_to_proto(view)
    assert plant.HasField("last_watered_at")
    assert plant.last_watered_at.ToDatetime(tzinfo=UTC) == NOW
    assert plant.removed is True
