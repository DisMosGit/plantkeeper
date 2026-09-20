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
    SpeciesCacheInvalidated,
    SpeciesSyncRequested,
    SpeciesUpdated,
)
from plantkeeper.domain.garden.events import PlantAdded, PlantMoved, PlantOnboarded, PlantRemoved
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.notifications.events import NotificationCreated, NotificationRead
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
}
"""The topic of every event in ``docs/events.md``."""

EVENT_TYPES: Final[dict[str, type[DomainEvent]]] = {event.__name__: event for event in EVENT_TOPICS}
"""Every event, keyed by the ``event_name`` a message carries.

A consumer reads the header, not the body, to decide what it is holding: the
body is the event document and says nothing about its own type, and a topic
carries every event of its context.
"""

DLQ_TOPIC: Final = "plantkeeper.dlq.v1"
"""Where the relay copies a message that exhausted its publish attempts."""

PARTITION_KEY_FIELDS: Final = ("plant_id", "household_id", "species_id", "sensor_id")
"""Payload fields tried, in order, when deriving a message key."""

HEADER_EVENT_NAME: Final = "event_name"
HEADER_EVENT_ID: Final = "event_id"
HEADER_ORIGINAL_TOPIC: Final = "original_topic"
HEADER_ERROR: Final = "error"


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
