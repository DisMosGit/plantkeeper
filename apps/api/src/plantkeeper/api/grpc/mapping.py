"""Conversions between application views and the protobuf messages.

The proto contract is a wire format, not the application's own shape: identifiers
stay wrapped messages, datetimes become ``google.protobuf.Timestamp`` and care
intervals ``google.protobuf.Duration``. Parsing is the one place a malformed
client value becomes a domain value error, which the servicers turn into
``INVALID_ARGUMENT``.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from google.protobuf import duration_pb2, timestamp_pb2

from plantkeeper.api.grpc.generated.plantkeeper.v1 import care_pb2, common_pb2, garden_pb2
from plantkeeper.application.views import CareScheduleView, PlantView
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SensorId, SpeciesId


def plant_id_to_proto(value: PlantId) -> common_pb2.PlantId:
    """Wrap a plant identifier."""
    return common_pb2.PlantId(value=str(value.value))


def household_id_to_proto(value: HouseholdId) -> common_pb2.HouseholdId:
    """Wrap a household identifier."""
    return common_pb2.HouseholdId(value=str(value.value))


def species_id_to_proto(value: SpeciesId) -> common_pb2.SpeciesId:
    """Wrap a species identifier."""
    return common_pb2.SpeciesId(value=str(value.value))


def sensor_id_to_proto(value: SensorId) -> common_pb2.SensorId:
    """Wrap a sensor identifier."""
    return common_pb2.SensorId(value=str(value.value))


def plant_id_from_proto(value: common_pb2.PlantId) -> PlantId:
    """Parse a plant identifier, raising ``ValidationError`` on a non-UUID.

    ``model_validate`` rather than the constructor: an identifier is a
    ``RootModel[UUID]``, so a string goes through the same validation a JSON body
    would, and a malformed value becomes the ``ValidationError`` the error table
    turns into ``INVALID_ARGUMENT``.
    """
    return PlantId.model_validate(value.value)


def household_id_from_proto(value: common_pb2.HouseholdId) -> HouseholdId:
    """Parse a household identifier, raising ``ValidationError`` on a non-UUID."""
    return HouseholdId.model_validate(value.value)


def timestamp_to_proto(value: datetime) -> timestamp_pb2.Timestamp:
    """Render an aware datetime as a protobuf timestamp.

    Protobuf 7's well-known types fill themselves in place through an instance
    method (``Timestamp().FromDatetime(dt)``), which is why this is three lines
    and not one expression.
    """
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(value)
    return timestamp


def duration_to_proto(value: timedelta) -> duration_pb2.Duration:
    """Render a timedelta as a protobuf duration."""
    duration = duration_pb2.Duration()
    duration.FromTimedelta(value)
    return duration


def care_schedule_to_proto(view: CareScheduleView) -> care_pb2.CareSchedule:
    """Project a care schedule view onto the wire."""
    return care_pb2.CareSchedule(
        plant_id=plant_id_to_proto(view.plant_id),
        watering_interval=duration_to_proto(view.watering_interval),
        next_watering_at=timestamp_to_proto(view.next_watering_at),
        version=view.version,
    )


def plant_to_proto(view: PlantView) -> garden_pb2.Plant:
    """Project a plant view onto the wire.

    ``last_watered_at`` is optional in the contract, so a plant that was never
    watered leaves the field unset rather than sending an epoch sentinel a client
    would have to special-case.
    """
    plant = garden_pb2.Plant(
        plant_id=plant_id_to_proto(view.plant_id),
        household_id=household_id_to_proto(view.household_id),
        species_id=species_id_to_proto(view.species_id),
        name=view.name,
        location=view.location,
        added_at=timestamp_to_proto(view.added_at),
        removed=view.removed,
    )
    if view.last_watered_at is not None:
        plant.last_watered_at.CopyFrom(timestamp_to_proto(view.last_watered_at))
    return plant
