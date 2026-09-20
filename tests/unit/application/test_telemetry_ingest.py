"""The telemetry ingress, without Kafka and without Postgres.

The consumer's whole contract is the order it does four things in: validate,
resolve, store, announce — and stop at the first one that fails. The fakes here
record every write so the test can assert not only what happened but what did
*not*: a malformed message must leave no reading and no event behind.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from types import TracebackType
from typing import Self, cast
from uuid import UUID

import pytest

from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.telemetry.ingest import TelemetryIngestConsumer, parse_raw_reading
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.telemetry.events import TelemetryReceived
from plantkeeper.domain.telemetry.reading import TelemetryReading
from plantkeeper.domain.telemetry.sensor import Sensor

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
SENSOR_ID = UUID("00000000-0000-0000-0000-0000000000f1")
PLANT_ID = UUID("00000000-0000-0000-0000-0000000000f2")


def a_raw_payload(
    *,
    sensor_id: UUID = SENSOR_ID,
    recorded_at: datetime = NOW,
    moisture: float = 42.5,
    temperature: float = 21.5,
    light: float = 1200.0,
) -> bytes:
    """The JSON a simulator produces, as bytes."""
    return json.dumps(
        {
            "sensor_id": str(sensor_id),
            "recorded_at": recorded_at.isoformat(),
            "moisture": moisture,
            "temperature": temperature,
            "light": light,
        }
    ).encode("utf-8")


class FakeSensors:
    """A sensor registry holding whatever the test registered."""

    def __init__(self, sensor: Sensor | None) -> None:
        self.sensor = sensor

    async def get(self, sensor_id: SensorId) -> Sensor | None:
        """Return the registered sensor, if the id matches."""
        if self.sensor is not None and self.sensor.id == sensor_id:
            return self.sensor
        return None


class FakeTelemetry:
    """An append-only reading store with the table's ``(sensor_id, recorded_at)`` key."""

    def __init__(self) -> None:
        self.readings: list[TelemetryReading] = []
        self.keys: set[tuple[UUID, datetime]] = set()

    async def add_many(self, readings: list[TelemetryReading]) -> int:
        """Insert what is new and report how many rows that was."""
        inserted = 0
        for reading in readings:
            key = (reading.sensor_id.value, reading.recorded_at)
            if key in self.keys:
                continue
            self.keys.add(key)
            self.readings.append(reading)
            inserted += 1
        return inserted

    async def list_by_plant(self, plant_id: PlantId, **_: object) -> list[TelemetryReading]:
        """Return the plant's readings, oldest first."""
        return [reading for reading in self.readings if reading.plant_id == plant_id]


class FakeOutbox:
    """The transactional outbox, recording the events appended to it."""

    def __init__(self) -> None:
        self.events: list[DomainEvent] = []

    async def append(self, event: DomainEvent) -> None:
        """Stage one event."""
        self.events.append(event)


class FakeUnitOfWork:
    """One transaction over the fake repositories."""

    def __init__(
        self,
        *,
        sensor: Sensor | None = None,
        telemetry: FakeTelemetry | None = None,
    ) -> None:
        self.sensors = FakeSensors(sensor)
        self.telemetry = telemetry if telemetry is not None else FakeTelemetry()
        self.outbox = FakeOutbox()
        self.commits = 0
        self.entered = 0
        self.rolled_back = 0
        self._committed = False

    async def __aenter__(self) -> Self:
        self.entered += 1
        self._committed = False
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Mirror the real unit of work: an uncommitted transaction rolls back."""
        if exc_type is not None or not self._committed:
            self.rolled_back += 1

    async def commit(self) -> None:
        """Count the commit and mark the transaction as landed."""
        self.commits += 1
        self._committed = True


class FakeClock:
    """A clock the test sets."""

    def __init__(self, now: datetime = NOW) -> None:
        self._now = now

    def now(self) -> datetime:
        """Return the instant the test has set."""
        return self._now


def a_registered_sensor() -> Sensor:
    """A sensor bound to the plant the tests publish for."""
    return Sensor(
        SensorId(SENSOR_ID),
        plant_id=PlantId(PLANT_ID),
        added_at=NOW,
    )


def a_consumer(unit_of_work: FakeUnitOfWork) -> TelemetryIngestConsumer:
    """The consumer under test, with a clock the test controls."""
    return TelemetryIngestConsumer(cast(UnitOfWork, unit_of_work), FakeClock())


# --- validation ---------------------------------------------------------------


def test_a_valid_message_validates_into_the_envelope() -> None:
    reading = parse_raw_reading(a_raw_payload())

    assert reading is not None
    assert reading.sensor_id == SENSOR_ID
    assert reading.recorded_at == NOW
    assert reading.moisture.value == 42.5
    assert reading.temperature.value == 21.5
    assert reading.light.value == 1200.0


def test_a_message_accepts_a_string_as_well_as_bytes() -> None:
    assert parse_raw_reading(a_raw_payload().decode("utf-8")) is not None


@pytest.mark.parametrize(
    "payload",
    [
        b"not json at all",
        b"[1, 2, 3]",
        b"{}",
        json.dumps(
            {
                "sensor_id": str(SENSOR_ID),
                "recorded_at": NOW.isoformat(),
                "moisture": 101.0,
                "temperature": 21.5,
                "light": 1200.0,
            }
        ).encode("utf-8"),
        json.dumps(
            {
                "sensor_id": "not-a-uuid",
                "recorded_at": "yesterday",
                "moisture": 10.0,
                "temperature": 21.5,
                "light": 1200.0,
            }
        ).encode("utf-8"),
        json.dumps(
            {
                "sensor_id": str(SENSOR_ID),
                "recorded_at": "2026-09-20T12:00:00",
                "moisture": 10.0,
                "temperature": 21.5,
                "light": 1200.0,
            }
        ).encode("utf-8"),
        json.dumps(
            {
                "sensor_id": str(SENSOR_ID),
                "recorded_at": NOW.isoformat(),
                "moisture": 10.0,
                "temperature": 21.5,
                "light": 1200.0,
                "extra": "surprise",
            }
        ).encode("utf-8"),
    ],
    ids=[
        "not-json",
        "not-an-object",
        "empty",
        "moisture-range",
        "not-uuids",
        "naive-time",
        "extra",
    ],
)
def test_a_message_that_cannot_be_believed_is_rejected(payload: bytes) -> None:
    assert parse_raw_reading(payload) is None


# --- the consumer -------------------------------------------------------------


async def test_a_valid_reading_is_stored_and_announced() -> None:
    uow = FakeUnitOfWork(sensor=a_registered_sensor())

    stored = await a_consumer(uow).ingest(a_raw_payload())

    assert stored is True
    assert len(uow.telemetry.readings) == 1
    reading = uow.telemetry.readings[0]
    assert reading.sensor_id == SensorId(SENSOR_ID)
    assert reading.plant_id == PlantId(PLANT_ID)
    assert reading.recorded_at == NOW
    assert uow.commits == 1

    assert len(uow.outbox.events) == 1
    event = uow.outbox.events[0]
    assert isinstance(event, TelemetryReceived)
    assert event.sensor_id == SensorId(SENSOR_ID)
    assert event.plant_id == PlantId(PLANT_ID)
    assert event.recorded_at == NOW
    # The event happened when the measurement did, not when it was received.
    assert event.occurred_at == NOW
    assert event.moisture.value == 42.5


@pytest.mark.parametrize(
    "payload",
    [b"not json at all", b"{}", a_raw_payload(moisture=250.0)],
    ids=["not-json", "empty", "out-of-range"],
)
async def test_a_message_that_cannot_be_believed_never_reaches_the_database(
    payload: bytes,
) -> None:
    uow = FakeUnitOfWork(sensor=a_registered_sensor())

    stored = await a_consumer(uow).ingest(payload)

    assert stored is False
    assert uow.telemetry.readings == []
    assert uow.outbox.events == []
    assert uow.commits == 0
    assert uow.entered == 0, "an unreadable message should not even take a transaction"


async def test_telemetry_from_an_unregistered_sensor_is_dropped() -> None:
    uow = FakeUnitOfWork(sensor=None)

    stored = await a_consumer(uow).ingest(a_raw_payload())

    assert stored is False
    assert uow.telemetry.readings == []
    assert uow.outbox.events == []
    assert uow.commits == 0


async def test_a_reading_for_another_sensor_does_not_exist_for_this_one() -> None:
    uow = FakeUnitOfWork(sensor=a_registered_sensor())

    stored = await a_consumer(uow).ingest(
        a_raw_payload(sensor_id=UUID("00000000-0000-0000-0000-0000000000f3"))
    )

    assert stored is False
    assert uow.telemetry.readings == []


async def test_a_redelivery_stores_nothing_new_and_announces_nothing_new() -> None:
    """The table's key is the idempotency: a second delivery changes nothing.

    The event is not appended twice either: it travelled in the transaction that
    stored the reading, and a second ``TelemetryReceived`` would carry a fresh
    ``event_id`` that no consumer-group ledger could recognise as a duplicate.
    """
    uow = FakeUnitOfWork(sensor=a_registered_sensor())
    consumer = a_consumer(uow)
    payload = a_raw_payload()

    first = await consumer.ingest(payload)
    second = await consumer.ingest(payload)

    assert (first, second) == (True, True), "both deliveries are accepted"
    assert len(uow.telemetry.readings) == 1
    assert len(uow.outbox.events) == 1
    assert uow.commits == 1
    assert uow.rolled_back == 1, "the redelivery's empty transaction is rolled back"


async def test_two_readings_of_the_same_sensor_are_both_stored() -> None:
    uow = FakeUnitOfWork(sensor=a_registered_sensor())
    consumer = a_consumer(uow)

    await consumer.ingest(a_raw_payload())
    await consumer.ingest(
        a_raw_payload(recorded_at=datetime(2026, 9, 20, 13, 0, tzinfo=UTC), moisture=40.0)
    )

    assert [reading.moisture.value for reading in uow.telemetry.readings] == [42.5, 40.0]


async def test_a_transient_failure_propagates_so_the_delivery_is_retried() -> None:
    class FailingTelemetry(FakeTelemetry):
        async def add_many(self, readings: Sequence[TelemetryReading]) -> int:
            """Fail the way a lost connection would."""
            raise ConnectionError("the database went away")

    uow = FakeUnitOfWork(sensor=a_registered_sensor(), telemetry=FailingTelemetry())

    with pytest.raises(ConnectionError):
        await a_consumer(uow).ingest(a_raw_payload())

    assert uow.rolled_back == 1
    assert uow.commits == 0
    assert uow.outbox.events == []
