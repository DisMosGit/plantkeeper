"""The telemetry ingress: raw sensor JSON in, stored readings and events out.

This is the only consumer that does not read a :class:`~plantkeeper.domain.base.DomainEvent`.
``telemetry.raw`` carries what a sensor actually said, which is nobody's domain
event yet: the simulator has no idea which plant a sensor watches, and a sensor
that has never been registered has no plant at all. The consumer therefore does
four things in order, and stops at the first one that says it cannot:

#. **validate** the raw document — a malformed or out-of-range message is
   acknowledged and dropped, never retried, because retrying it cannot help;
#. **resolve** the sensor through the registry, which is how ``plant_id`` enters
   the picture; an unregistered sensor is dropped with a warning, and the next
   reading lands as soon as the sensor is registered;
#. **store** the reading in ``write_telemetry.sensor_readings``. The table is
   keyed by ``(sensor_id, recorded_at)``, so a redelivery, a replay or a second
   simulator run is absorbed by the database rather than by a check here; a
   delivery whose row is already there announces nothing either, because its
   event travelled in the transaction that stored it;
#. **append** the events the new reading produces to the outbox, in the same
   transaction as the reading. The domain's own :meth:`Sensor.record` decides what
   a measurement means — :class:`TelemetryReceived` always, and the threshold
   event when the reading crosses one — and the ingress appends every event the
   aggregate raised. The outbox relay publishes them, which is what keeps the
   sagas' input on the same at-least-once, one-writer path as every other domain
   event.

The sensor row itself is deliberately not written: ``record`` updates the
aggregate's ``last_seen_at`` in memory, and Phase 5 decided that a reading is not
worth an ``UPDATE`` on the sensor (the readings table already answers "when did
this sensor last report"). What the aggregate is called for here is its
*decisions*, not its state.

The transactional ordering matters in both directions: a crash before the commit
loses nothing (Kafka redelivers), and a crash after it re-inserts nothing (the
key already holds).
"""

from __future__ import annotations

import json
import logging
from typing import Final
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, ValidationError

from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.domain.identifiers import PlantId, SensorId
from plantkeeper.domain.telemetry.reading import TelemetryReading
from plantkeeper.domain.values import LightLevel, Moisture, SensorReading, Temperature

logger = logging.getLogger(__name__)

PAYLOAD_SNIPPET_LIMIT: Final = 200
"""How much of an unparseable message is quoted in the log."""


class RawReading(BaseModel):
    """One raw measurement, in the shape the ``telemetry.raw`` producer sends.

    The ranges are the domain's own value objects, not copies of them, so a
    message that passes here cannot fail later when the event is built. That is
    the deliberate coupling: "moisture is a percentage" has one definition in
    this repository and it lives in the domain.
    """

    model_config = ConfigDict(extra="forbid")

    sensor_id: UUID
    recorded_at: AwareDatetime
    moisture: Moisture
    temperature: Temperature
    light: LightLevel


def parse_raw_reading(payload: bytes | str) -> RawReading | None:
    """Validate one raw message, returning ``None`` when it is unusable.

    ``None`` rather than an exception: a contract violation is a routing decision
    for the consumer (log it, acknowledge it, move on), while an exception would
    stop the partition behind a message that can never be handled.
    """
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
    try:
        document = json.loads(text)
    except ValueError:
        logger.warning("telemetry message is not JSON: %s", _snippet(text))
        return None
    if not isinstance(document, dict):
        logger.warning("telemetry message is not a JSON object: %s", _snippet(text))
        return None
    try:
        return RawReading.model_validate(document)
    except ValidationError as exc:
        logger.warning(
            "telemetry message does not validate (%s): %s", exc.error_count(), _snippet(text)
        )
        return None


def _snippet(text: str) -> str:
    """Return a short, single-line excerpt of a message for a log line."""
    flattened = " ".join(text.split())
    if len(flattened) <= PAYLOAD_SNIPPET_LIMIT:
        return flattened
    return f"{flattened[:PAYLOAD_SNIPPET_LIMIT]}…"


class TelemetryIngestConsumer:
    """The consumer group that turns raw telemetry into domain facts."""

    name = "telemetry-ingest"
    """The group's suffix: ``<prefix>`` is the full consumer group, because this
    consumer reads a topic of its own rather than one shared with the sagas."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock

    @property
    def unit_of_work(self) -> UnitOfWork:
        """The transaction :meth:`ingest` is running inside."""
        return self._unit_of_work

    async def ingest(self, payload: bytes | str) -> bool:
        """Validate, resolve, store and announce one raw message.

        Returns ``True`` when the reading is in the table — stored by this
        delivery or already there — and ``False`` when the message was dropped
        (unparseable, or from a sensor nobody registered). Raises whatever a
        transient failure raises: the delivery is then redelivered, and the
        readings table's key makes the retry safe.
        """
        reading = parse_raw_reading(payload)
        if reading is None:
            return False

        sensor = await self._unit_of_work.sensors.get(SensorId(reading.sensor_id))
        if sensor is None:
            logger.warning(
                "dropping telemetry from unregistered sensor %s recorded at %s",
                reading.sensor_id,
                reading.recorded_at.isoformat(),
            )
            return False

        stored = TelemetryReading(
            sensor_id=SensorId(reading.sensor_id),
            plant_id=PlantId(sensor.plant_id.value),
            recorded_at=reading.recorded_at,
            moisture=reading.moisture,
            temperature=reading.temperature,
            light=reading.light,
        )
        async with self._unit_of_work:
            inserted = await self._unit_of_work.telemetry.add_many([stored])
            if inserted:
                # The aggregate decides what the measurement means: always
                # ``TelemetryReceived``, plus the threshold event it crossed. The
                # events are drained and appended explicitly because the sensor row
                # is not being saved, so nothing would collect them otherwise.
                sensor.record(
                    SensorReading(
                        sensor_id=stored.sensor_id,
                        recorded_at=stored.recorded_at,
                        moisture=stored.moisture,
                        temperature=stored.temperature,
                        light=stored.light,
                    )
                )
                for event in sensor.collect_events():
                    await self._unit_of_work.outbox.append(event)
                await self._unit_of_work.commit()

        if not inserted:
            # The reading was already stored by an earlier delivery, whose event
            # is in the outbox with it. Announcing this one too would put a second
            # ``TelemetryReceived`` with a new ``event_id`` on the topic — a
            # duplicate no consumer-group ledger can recognise — so the delivery
            # changes nothing and the transaction is left to roll back.
            logger.debug(
                "reading of sensor %s at %s was already stored; nothing announced",
                stored.sensor_id,
                stored.recorded_at.isoformat(),
            )
        return True
