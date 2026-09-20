"""Boundary tests for the shared scalar value objects."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel, ValidationError

from plantkeeper.domain.identifiers import SensorId
from plantkeeper.domain.values import (
    CareInterval,
    CareRule,
    FertilizingInterval,
    LightLevel,
    Location,
    Moisture,
    RepottingInterval,
    SensorReading,
    Temperature,
    WateringInterval,
)

LOCATION_MAX_LENGTH = 100


def test_location_strips_surrounding_whitespace() -> None:
    assert Location(value="  kitchen  ").value == "kitchen"


@pytest.mark.parametrize("raw", ["a", "a" * LOCATION_MAX_LENGTH])
def test_location_accepts_length_boundaries(raw: str) -> None:
    assert Location(value=raw).value == raw


@pytest.mark.parametrize("raw", ["", "   "])
def test_location_rejects_empty_values(raw: str) -> None:
    with pytest.raises(ValidationError):
        Location(value=raw)


def test_location_rejects_values_over_the_maximum_length() -> None:
    with pytest.raises(ValidationError):
        Location(value="a" * (LOCATION_MAX_LENGTH + 1))


def test_location_rejects_a_non_string() -> None:
    with pytest.raises(ValidationError):
        Location.model_validate(42)


def test_location_is_frozen() -> None:
    location = Location(value="kitchen")

    with pytest.raises(ValidationError):
        location.__setattr__("value", "balcony")


@pytest.mark.parametrize("value", [0.0, 55.5, 100.0])
def test_moisture_accepts_in_range_values(value: float) -> None:
    assert Moisture(value=value).value == value


@pytest.mark.parametrize("value", [-0.01, 100.01])
def test_moisture_rejects_out_of_range_values(value: float) -> None:
    with pytest.raises(ValidationError):
        Moisture(value=value)


@pytest.mark.parametrize("value", [-50.0, 21.5, 60.0])
def test_temperature_accepts_in_range_values(value: float) -> None:
    assert Temperature(value=value).value == value


@pytest.mark.parametrize("value", [-50.01, 60.01])
def test_temperature_rejects_out_of_range_values(value: float) -> None:
    with pytest.raises(ValidationError):
        Temperature(value=value)


def test_light_level_accepts_zero_and_positive_values() -> None:
    assert LightLevel(value=0.0).value == 0.0
    assert LightLevel(value=12_000.0).value == 12_000.0


def test_light_level_rejects_negative_values() -> None:
    with pytest.raises(ValidationError):
        LightLevel(value=-0.1)


INTERVAL_TYPES: list[type[CareInterval]] = [
    WateringInterval,
    FertilizingInterval,
    RepottingInterval,
]


@pytest.mark.parametrize("interval_type", INTERVAL_TYPES)
def test_care_interval_accepts_a_positive_timedelta(interval_type: type[CareInterval]) -> None:
    interval = interval_type(value=timedelta(hours=6))

    assert interval.value == timedelta(hours=6)


@pytest.mark.parametrize("interval_type", INTERVAL_TYPES)
@pytest.mark.parametrize("value", [timedelta(0), timedelta(seconds=-1)])
def test_care_interval_rejects_non_positive_timedeltas(
    interval_type: type[CareInterval], value: timedelta
) -> None:
    with pytest.raises(ValidationError):
        interval_type(value=value)


def test_care_interval_subclasses_are_distinct_types() -> None:
    # Compared as ``object``: the point is that Pydantic keeps the subclasses
    # distinct, so a direct comparison is (correctly) a type error.
    watering: object = WateringInterval(value=timedelta(days=7))
    fertilizing: object = FertilizingInterval(value=timedelta(days=7))

    assert watering != fertilizing


def test_care_rule_combines_the_three_cadences() -> None:
    rule = CareRule(
        watering=WateringInterval(value=timedelta(days=7)),
        fertilizing=FertilizingInterval(value=timedelta(days=30)),
        repotting=RepottingInterval(value=timedelta(days=182)),
    )

    assert rule.watering.value == timedelta(days=7)
    assert rule.fertilizing.value == timedelta(days=30)
    assert rule.repotting.value == timedelta(days=182)


def test_care_rule_requires_every_cadence() -> None:
    with pytest.raises(ValidationError):
        CareRule.model_validate({"watering": {"value": 86400}})


def test_care_rule_is_frozen() -> None:
    rule = CareRule(
        watering=WateringInterval(value=timedelta(days=7)),
        fertilizing=FertilizingInterval(value=timedelta(days=30)),
        repotting=RepottingInterval(value=timedelta(days=182)),
    )

    with pytest.raises(ValidationError):
        rule.__setattr__("watering", WateringInterval(value=timedelta(days=1)))


def _make_reading(recorded_at: datetime) -> SensorReading:
    return SensorReading(
        sensor_id=SensorId.new(),
        recorded_at=recorded_at,
        moisture=Moisture(value=42.0),
        temperature=Temperature(value=21.5),
        light=LightLevel(value=800.0),
    )


def test_sensor_reading_accepts_an_aware_timestamp() -> None:
    reading = _make_reading(datetime(2026, 1, 1, 12, 0, tzinfo=UTC))

    assert reading.recorded_at.tzinfo is not None


def test_sensor_reading_rejects_a_naive_timestamp() -> None:
    with pytest.raises(ValidationError):
        _make_reading(datetime(2026, 1, 1, 12, 0))


def test_sensor_reading_validates_nested_measurements() -> None:
    with pytest.raises(ValidationError):
        SensorReading.model_validate(
            {
                "sensor_id": str(SensorId.new()),
                "recorded_at": "2026-01-01T12:00:00Z",
                "moisture": {"value": 200},
                "temperature": {"value": 21.5},
                "light": {"value": 800},
            }
        )


def test_sensor_reading_survives_a_json_round_trip() -> None:
    reading = _make_reading(datetime(2026, 1, 1, 12, 0, tzinfo=UTC))

    assert SensorReading.model_validate_json(reading.model_dump_json()) == reading


def test_sensor_reading_is_frozen() -> None:
    reading = _make_reading(datetime(2026, 1, 1, 12, 0, tzinfo=UTC))

    with pytest.raises(ValidationError):
        reading.__setattr__("moisture", Moisture(value=10.0))


def as_json(value: BaseModel) -> object:
    """Return how ``value`` looks on the wire.

    Loosely typed on purpose: Pydantic declares ``model_dump`` as returning a
    mapping, but a scalar value object serialises as a scalar, and the point of
    these tests is to pin that down rather than to agree with the declaration.
    """
    return value.model_dump(mode="json")


def test_scalar_values_serialise_as_their_scalar() -> None:
    """A wrapper is a domain type, not a wire shape (see ``docs/events.md``)."""
    assert as_json(Location(value="Shelf")) == "Shelf"
    assert Location(value="Shelf").model_dump_json() == '"Shelf"'
    assert as_json(Moisture(value=42.0)) == 42.0
    assert as_json(Temperature(value=21.5)) == 21.5
    assert as_json(LightLevel(value=800.0)) == 800.0
    assert as_json(WateringInterval(value=timedelta(days=7))) == "P7D"


def test_scalar_values_still_accept_the_wrapped_shape() -> None:
    """The pre-Phase-2 shape keeps validating, so a stored row still loads."""
    assert Location.model_validate({"value": "Shelf"}) == Location(value="Shelf")
    assert Moisture.model_validate({"value": 42.0}) == Moisture(value=42.0)
    assert WateringInterval.model_validate({"value": 86_400}) == WateringInterval(
        value=timedelta(days=1)
    )


def test_scalar_values_accept_the_bare_scalar() -> None:
    assert Location.model_validate("Shelf") == Location(value="Shelf")
    assert Moisture.model_validate(42.0) == Moisture(value=42.0)
    assert WateringInterval.model_validate(86_400) == WateringInterval(value=timedelta(days=1))


def test_scalar_values_survive_a_json_round_trip() -> None:
    for value in (
        Location(value="Shelf"),
        Moisture(value=42.0),
        Temperature(value=21.5),
        LightLevel(value=800.0),
        WateringInterval(value=timedelta(days=7)),
    ):
        assert type(value).model_validate_json(value.model_dump_json()) == value
