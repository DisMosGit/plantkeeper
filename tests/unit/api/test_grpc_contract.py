"""The generated stubs must express the contract ``docs/grpc.md`` promises.

``make proto`` creates these modules from ``proto/`` and they are not committed,
so this is the test that fails when generation was skipped, stale, or pointed at
the wrong package path.
"""

from __future__ import annotations

from google.protobuf import timestamp_pb2

from plantkeeper.api.grpc.generated.plantkeeper.v1 import (
    care_pb2,
    common_pb2,
    garden_pb2,
)


def test_the_care_service_exposes_both_rpcs() -> None:
    service = care_pb2.DESCRIPTOR.services_by_name["CareService"]
    assert service.full_name == "plantkeeper.v1.CareService"
    assert [method.name for method in service.methods] == ["GetTodayCare", "CompleteWatering"]


def test_the_garden_service_exposes_both_rpcs() -> None:
    service = garden_pb2.DESCRIPTOR.services_by_name["GardenService"]
    assert service.full_name == "plantkeeper.v1.GardenService"
    assert [method.name for method in service.methods] == ["ListPlants", "GetPlant"]


def test_the_shared_reading_round_trips() -> None:
    """``SensorReading`` has no RPC yet; this keeps it a real, usable message."""
    reading = common_pb2.SensorReading(
        sensor_id=common_pb2.SensorId(value="sensor-1"),
        moisture=12.5,
        temperature=21.0,
        light=400.0,
    )
    reading.recorded_at.CopyFrom(timestamp_pb2.Timestamp(seconds=1_767_268_800))
    parsed = common_pb2.SensorReading.FromString(reading.SerializeToString())
    assert parsed.sensor_id.value == "sensor-1"
    assert parsed.recorded_at.seconds == 1_767_268_800
    assert parsed.moisture == 12.5
    assert parsed.light == 400.0
