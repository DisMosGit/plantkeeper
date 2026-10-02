"""Kafka topics and message keys.

The layout is **one topic per bounded context**, not one topic per event type.
A context publishes the events it owns to its own topic, and a consumer that
cares about three Garden events subscribes once and dispatches on the
``event_name`` header. Twenty-one topics would push the broker's metadata cost
onto every consumer for no benefit at this scale.

The message contract is deliberately thin:

* body — the event's own JSON document (``event.model_dump(mode="json")``), the
  same shape ``docs/events.md`` documents and the outbox stores;
* headers — ``event_name`` and ``event_id``, so a consumer can deserialise
  without guessing and can deduplicate on ``(consumer_group, event_id)``;
* key — an aggregate-derived key (:func:`partition_key_for`), so every event of
  one plant lands in one partition and therefore stays in order.
"""

from __future__ import annotations

from typing import Final

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
    SpeciesAdded,
    SpeciesCacheInvalidated,
    SpeciesSyncRequested,
    SpeciesUpdated,
)
from plantkeeper.domain.garden.events import PlantAdded, PlantMoved, PlantOnboarded, PlantRemoved
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.notifications.events import NotificationCreated, NotificationRead
from plantkeeper.domain.saga.events import (
    SagaCompensated,
    SagaCompleted,
    SagaFailed,
    SagaParked,
    SagaRetrying,
    SagaStarted,
)
from plantkeeper.domain.telemetry.events import (
    SensorOffline,
    SoilMoistureHigh,
    SoilMoistureLow,
    TelemetryReceived,
    TemperatureAnomaly,
)

GARDEN_EVENTS: Final = "garden.events"
CARE_EVENTS: Final = "care.events"
CATALOG_EVENTS: Final = "catalog.events"
JOURNAL_EVENTS: Final = "journal.events"
TELEMETRY_EVENTS: Final = "telemetry.events"
NOTIFICATIONS_EVENTS: Final = "notifications.events"
SAGA_EVENTS: Final = "saga.events"

TELEMETRY_RAW: Final = "telemetry.raw"
"""Where the IoT simulator publishes, and the only topic that is not a domain event.

Its messages are raw sensor JSON — ``sensor_id``, ``recorded_at``, ``moisture``,
``temperature``, ``light`` — published by ``tools/iot-simulator`` rather than by
the outbox relay. They are ingress, not history: the telemetry consumer validates
them, stores the reading and appends the ``TelemetryReceived`` that belongs in the
event catalogue to its **transactional outbox**, so no domain event ever travels
this topic. ``docs/telemetry.md`` holds the envelope.
"""

EVENT_TOPICS: Final[dict[type[DomainEvent], str]] = {
    # Garden
    PlantAdded: GARDEN_EVENTS,
    PlantRemoved: GARDEN_EVENTS,
    PlantMoved: GARDEN_EVENTS,
    PlantOnboarded: GARDEN_EVENTS,
    # Care
    CareScheduleCreated: CARE_EVENTS,
    WateringDue: CARE_EVENTS,
    WateringCompleted: CARE_EVENTS,
    WateringRescheduled: CARE_EVENTS,
    CareMissed: CARE_EVENTS,
    CareSkipped: CARE_EVENTS,
    # Catalog
    SpeciesSyncRequested: CATALOG_EVENTS,
    SpeciesAdded: CATALOG_EVENTS,
    SpeciesUpdated: CATALOG_EVENTS,
    SpeciesCacheInvalidated: CATALOG_EVENTS,
    # Journal
    JournalEntryAdded: JOURNAL_EVENTS,
    # Telemetry
    TelemetryReceived: TELEMETRY_EVENTS,
    SoilMoistureLow: TELEMETRY_EVENTS,
    SoilMoistureHigh: TELEMETRY_EVENTS,
    TemperatureAnomaly: TELEMETRY_EVENTS,
    SensorOffline: TELEMETRY_EVENTS,
    # Notifications
    NotificationCreated: NOTIFICATIONS_EVENTS,
    NotificationRead: NOTIFICATIONS_EVENTS,
    # Saga / system
    SagaStarted: SAGA_EVENTS,
    SagaCompleted: SAGA_EVENTS,
    SagaFailed: SAGA_EVENTS,
    SagaCompensated: SAGA_EVENTS,
    SagaRetrying: SAGA_EVENTS,
    SagaParked: SAGA_EVENTS,
}
"""The topic of every event in ``docs/events.md``."""

EVENT_TYPES: Final[dict[str, type[DomainEvent]]] = {event.__name__: event for event in EVENT_TOPICS}
"""Every event, keyed by the ``event_name`` a message carries.

A consumer reads the header, not the body, to decide what it is holding: the
body is the event document and says nothing about its own type, and a topic
carries every event of its context.
"""

DLQ_TOPIC: Final = "plantkeeper.dlq.v1"
"""Where a message that exhausted its attempts is copied.

One topic for both writers: the relay abandons a row it could not publish, and a
consumer moves aside a delivery it could not handle. Both copies carry
``original_topic`` and ``error``; a consumer's also names the ``consumer_group``,
so ``tools/dlq.py`` can clear the right ledger claim before replaying it.
"""

PARTITION_KEY_FIELDS: Final = ("plant_id", "household_id", "species_id", "sensor_id", "saga_id")
"""Payload fields tried, in order, when deriving a message key.

``saga_id`` comes last: a saga lifecycle event carries no aggregate identifier, and
keying it by the saga keeps its four messages in one partition, in order.
"""

HEADER_EVENT_NAME: Final = "event_name"
HEADER_EVENT_ID: Final = "event_id"
HEADER_ORIGINAL_TOPIC: Final = "original_topic"
HEADER_ERROR: Final = "error"
HEADER_CONSUMER_GROUP: Final = "consumer_group"
"""Which consumer group moved a dead-lettered delivery aside.

The relay's copies carry ``original_topic`` and ``error`` because a row belongs to
no group; a consumer's copies name the group as well, because a replay has to
clear that group's ledger claim — the only thing that stops the redelivery from
being recognised as a duplicate. See ``tools/dlq.py``.
"""

# Provenance. Every published message carries these beside ``event_name`` and
# ``event_id``, so a chain of reactions can be followed from any link and an
# event's origin is never anonymous (``docs/events.md``, ADR 0010).
HEADER_CORRELATION_ID: Final = "correlation_id"
HEADER_CAUSATION_ID: Final = "causation_id"
HEADER_RAISED_BY: Final = "raised_by"
HEADER_SCHEMA_VERSION: Final = "schema_version"
HEADER_TRACEPARENT: Final = "traceparent"
HEADER_OCCURRED_AT: Final = "occurred_at"
"""When the edge that raised the event observed it.

Distinct from the body's own ``occurred_at``: the body's is the aggregate's
instant, this is the delivery's. A consumer propagates *this* one, so the events
one handler raises share an instant even when the aggregates that produced them
were touched at different times.
"""

PROVENANCE_HEADERS: Final = (
    HEADER_CORRELATION_ID,
    HEADER_CAUSATION_ID,
    HEADER_RAISED_BY,
    HEADER_SCHEMA_VERSION,
    HEADER_TRACEPARENT,
    HEADER_OCCURRED_AT,
)
"""The provenance header names, for a test or a contract to assert the whole set."""

# The HTTP header a client or an upstream proxy uses to join a trace. Carried
# opaquely: the platform runs no tracer, it only passes the value through.
TRACEPARENT_HTTP_HEADER: Final = "traceparent"

CORRELATION_HTTP_HEADER: Final = "x-correlation-id"
"""The HTTP header a client may send to join an existing conversation."""


class UnmappedEventError(Exception):
    """An event type has no topic.

    Raised when the outbox is appended to. Failing here beats publishing an
    event to a guess and having every consumer silently ignore it.
    """


def topic_for(event: DomainEvent) -> str:
    """Return the topic ``event`` belongs to."""
    topic = EVENT_TOPICS.get(type(event))
    if topic is None:
        raise UnmappedEventError(f"no Kafka topic is mapped for {type(event).__name__}")
    return topic


def event_type_for(event_name: str) -> type[DomainEvent] | None:
    """Return the event class an ``event_name`` header names, if it is known.

    ``None`` rather than an exception: an unknown event is a routing concern for
    the consumer (it may be a contract violation or a newer producer), and the
    caller decides whether to skip it or fail.
    """
    return EVENT_TYPES.get(event_name)


def partition_key_for(event: DomainEvent) -> str:
    """Return the Kafka key that keeps one aggregate's events ordered.

    Every catalogued event carries at least one identifier of the aggregate it
    concerns; an event that carries none is keyed by its own ``event_id``, which
    still spreads messages over partitions — it only gives up intra-aggregate
    ordering, which such an event has no aggregate to order by.
    """
    payload = event.model_dump()
    for field in PARTITION_KEY_FIELDS:
        value = payload.get(field)
        if value is not None:
            return str(value)
    return str(event.event_id)
