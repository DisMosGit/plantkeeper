"""Unit tests for the infrastructure half of the consumer failure policy.

Two things have to be right before a subscriber can use the policy: the settings
have to become the budget they claim to be, and a Kafka delivery has to become the
copy the dead-letter publisher stores. The second is the one that fails silently —
a copy missing its topic or its key still lands on the topic, and an operator only
finds out when a replay goes to the wrong place.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from aiokafka import ConsumerRecord
from faststream.kafka import KafkaMessage
from faststream.kafka.message import ConsumerProtocol

from plantkeeper.domain.garden.events import PlantAdded
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SpeciesId
from plantkeeper.domain.values import Location
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.messaging.failures import (
    ACK_POLICY,
    dead_letter_for,
    failure_policy,
)
from plantkeeper.infrastructure.messaging.publisher import KafkaEventPublisher
from plantkeeper.infrastructure.messaging.topics import (
    DLQ_TOPIC,
    GARDEN_EVENTS,
    HEADER_CONSUMER_GROUP,
    HEADER_ERROR,
    HEADER_EVENT_ID,
    HEADER_EVENT_NAME,
    HEADER_ORIGINAL_TOPIC,
)


def a_plant_added() -> PlantAdded:
    """One valid Garden event, whose JSON form is a valid message body."""
    return PlantAdded(
        plant_id=PlantId.new(),
        household_id=HouseholdId.new(),
        species_id=SpeciesId.new(),
        name="Fern",
        location=Location(value="Shelf"),
        added_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    )


def a_message(
    event: PlantAdded,
    *,
    topic: str = GARDEN_EVENTS,
    keyed: bool = True,
) -> KafkaMessage:
    """Build the delivery FastStream would hand a subscriber.

    Keyed by default, because that is how the relay publishes an event: the
    aggregate's identifier keeps its events in one partition. ``keyed=False``
    stands for a message that was published without a key.
    """
    record = ConsumerRecord(
        topic=topic,
        partition=0,
        offset=12,
        timestamp=0,
        timestamp_type=0,
        key=str(event.plant_id).encode("utf-8") if keyed else None,
        value=event.model_dump_json().encode("utf-8"),
        headers=[],
        checksum=None,
        serialized_key_size=-1,
        serialized_value_size=-1,
    )
    return KafkaMessage(
        cast("Any", record),
        record.value,
        headers={"event_name": type(event).__name__, "event_id": str(event.event_id)},
        consumer=cast("ConsumerProtocol", None),
    )


class FakeBroker:
    """Records what a publish asked Kafka for."""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []

    async def publish(
        self,
        body: str,
        *,
        topic: str,
        key: bytes,
        headers: dict[str, str],
    ) -> None:
        """Record one publish."""
        self.published.append({"body": body, "topic": topic, "key": key, "headers": headers})


def test_the_settings_become_the_retry_budget() -> None:
    policy = failure_policy(
        Settings(
            consumer_max_attempts=7,
            consumer_retry_initial_wait_seconds=0.25,
            consumer_retry_max_wait_seconds=3.0,
        )
    )

    assert policy.max_attempts == 7
    assert policy.wait_before_retry(1) == 0.25
    assert policy.wait_before_retry(2) == 0.5
    assert policy.wait_before_retry(9) == 3.0


def test_the_default_acknowledgement_redelivers_a_failed_delivery() -> None:
    """``ACK_FIRST`` would commit the offset from the broker client's own timer."""
    from faststream.middlewares import AckPolicy

    assert ACK_POLICY is AckPolicy.NACK_ON_ERROR


def test_a_delivery_becomes_a_copy_of_itself() -> None:
    """The copy carries the message as it arrived, not a re-rendering of it."""
    event = a_plant_added()
    message = a_message(event)

    copy = dead_letter_for(message, consumer_group="plantkeeper-worker-journal-entries", error="X")

    assert copy.consumer_group == "plantkeeper-worker-journal-entries"
    assert copy.original_topic == GARDEN_EVENTS
    assert copy.partition_key == str(event.plant_id)
    assert copy.body == event.model_dump_json()
    assert copy.headers[HEADER_EVENT_NAME] == "PlantAdded"
    assert copy.headers[HEADER_EVENT_ID] == str(event.event_id)
    assert copy.error == "X"


def test_a_message_published_without_a_key_copies_without_one() -> None:
    """An unkeyed message must not gain a key on the way to the dead-letter topic."""
    message = a_message(a_plant_added(), keyed=False)

    copy = dead_letter_for(message, consumer_group="g", error="X")

    assert copy.partition_key == ""


async def test_the_copy_reaches_the_dead_letter_topic_with_its_origin() -> None:
    """The three facts a replay needs travel in the headers."""
    event = a_plant_added()
    broker = FakeBroker()
    publisher = KafkaEventPublisher(cast("Any", broker))
    copy = dead_letter_for(
        a_message(event), consumer_group="plantkeeper-worker-journal-entries", error="Boom: gone"
    )

    await publisher.publish_moved_aside(copy)

    assert len(broker.published) == 1
    published = broker.published[0]
    assert published["topic"] == DLQ_TOPIC
    assert published["key"] == str(event.plant_id).encode("utf-8")
    assert published["body"] == event.model_dump_json()
    assert published["headers"][HEADER_CONSUMER_GROUP] == "plantkeeper-worker-journal-entries"
    assert published["headers"][HEADER_ORIGINAL_TOPIC] == GARDEN_EVENTS
    assert published["headers"][HEADER_ERROR] == "Boom: gone"
    # The delivery's own headers travel too, so the copy is still traceable.
    assert published["headers"][HEADER_EVENT_ID] == str(event.event_id)
