"""The worker's subscription registration, without a broker.

``register_consumers`` decides which consumer group listens to which topic. Two
mistakes are easy and silent: subscribing a consumer twice (it would receive every
event twice) or deriving the wrong group id (the ledger and the offsets would
belong to the wrong consumer). A fake broker records the calls so both are pinned.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import cast

from dishka import AsyncContainer
from faststream.kafka import KafkaBroker, KafkaMessage

from plantkeeper.application.sagas.registry import WORKER_CONSUMER_TYPES
from plantkeeper.domain.garden.events import PlantAdded, PlantMoved
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.messaging.topics import EVENT_TOPICS, TELEMETRY_RAW
from plantkeeper.workers.consumers import (
    TELEMETRY_INGEST_OFFSET_RESET,
    register_consumers,
    register_telemetry_ingest,
    topics_for,
)

Subscriber = Callable[[KafkaMessage], Awaitable[None]]


class FakeBroker:
    """Records ``(topic, group_id, title, auto_offset_reset, handler)`` per subscription."""

    def __init__(self) -> None:
        self.routes: list[tuple[str, str, str, str, Subscriber]] = []

    def subscriber(
        self,
        topic: str,
        *,
        group_id: str,
        auto_offset_reset: str,
        title: str,
        description: str,
    ) -> Callable[[Subscriber], Subscriber]:
        """Stand in for FastStream's decorator-returning ``subscriber``."""
        del description  # Only the title keys the generated channel.

        def decorator(handler: Subscriber) -> Subscriber:
            self.routes.append((topic, group_id, title, auto_offset_reset, handler))
            return handler

        return decorator


def test_topics_for_deduplicates_events_of_one_context() -> None:
    """A consumer of two Garden events subscribes to ``garden.events`` once."""
    assert topics_for((PlantAdded, PlantMoved)) == ("garden.events",)


def test_every_consumer_is_subscribed_to_each_of_its_topics_once() -> None:
    broker = FakeBroker()

    register_consumers(
        cast("KafkaBroker", broker),
        container=cast("AsyncContainer", None),
        settings=Settings(worker_consumer_group_prefix="test-worker"),
    )

    subscribed = {(topic, group_id) for topic, group_id, _, _, _ in broker.routes}
    expected = {
        (EVENT_TOPICS[event_type], f"test-worker-{consumer_type.name}")
        for consumer_type in WORKER_CONSUMER_TYPES
        for event_type in consumer_type.handled_types
    }
    assert subscribed == expected


def test_every_subscription_has_a_unique_asyncapi_title() -> None:
    """One Kafka topic is consumed by several groups; the title keeps them apart.

    FastStream keys a generated AsyncAPI channel by the subscription's title and
    otherwise falls back to the handler's function name, which every handler here
    shares, so a duplicate title silently drops a channel from the document.
    """
    broker = FakeBroker()

    register_consumers(
        cast("KafkaBroker", broker),
        container=cast("AsyncContainer", None),
        settings=Settings(worker_consumer_group_prefix="test-worker"),
    )

    titles = [title for _, _, title, _, _ in broker.routes]
    assert len(titles) == len(set(titles))
    for topic, group_id, title, _, _ in broker.routes:
        assert title == f"{topic} to {group_id}"


def test_every_subscription_replays_from_the_beginning() -> None:
    """The ledger makes a replay safe, and a rebuilt consumer depends on it."""
    broker = FakeBroker()

    register_consumers(
        cast("KafkaBroker", broker),
        container=cast("AsyncContainer", None),
        settings=Settings(worker_consumer_group_prefix="test-worker"),
    )

    assert {auto_offset_reset for _, _, _, auto_offset_reset, _ in broker.routes} == {"earliest"}


def test_the_telemetry_ingress_is_subscribed_to_the_raw_topic_alone() -> None:
    """Raw telemetry is not a domain event, so it is registered on its own."""
    broker = FakeBroker()
    settings = Settings(
        telemetry_raw_topic=TELEMETRY_RAW,
        telemetry_ingest_consumer_group="test-telemetry-ingest",
    )

    register_telemetry_ingest(
        cast("KafkaBroker", broker), container=cast("AsyncContainer", None), settings=settings
    )

    assert len(broker.routes) == 1
    topic, group_id, _title, _auto_offset_reset, _handler = broker.routes[0]
    assert (topic, group_id) == (TELEMETRY_RAW, "test-telemetry-ingest")


def test_the_telemetry_ingress_starts_at_the_live_edge() -> None:
    """Raw telemetry has no ledger to replay against; the readings table is one."""
    broker = FakeBroker()

    register_telemetry_ingest(
        cast("KafkaBroker", broker),
        container=cast("AsyncContainer", None),
        settings=Settings(),
    )

    assert TELEMETRY_INGEST_OFFSET_RESET == "latest"
    assert broker.routes[0][3] == "latest"


def test_the_raw_topic_is_not_in_the_event_catalogue() -> None:
    """A topic that carries raw sensor JSON must not be taken for an event topic."""
    assert TELEMETRY_RAW not in set(EVENT_TOPICS.values())
