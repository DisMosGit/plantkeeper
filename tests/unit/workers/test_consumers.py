"""The worker's subscription registration, without a broker.

``register_consumers`` decides which consumer group listens to which topic. Two
mistakes are easy and silent: subscribing a consumer twice (it would receive every
event twice) or deriving the wrong group id (the ledger and the offsets would
belong to the wrong consumer). A fake broker records the calls so both are pinned.

The acknowledgement policy is pinned here too: it is the half of the failure
policy FastStream owns, and the default would commit a failed delivery's offset
from aiokafka's own timer — a delivery that is neither handled nor offered again.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import cast

from dishka import AsyncContainer
from faststream.kafka import KafkaBroker, KafkaMessage
from faststream.middlewares import AckPolicy

from plantkeeper.application.sagas.registry import WORKER_CONSUMER_TYPES
from plantkeeper.domain.garden.events import PlantAdded, PlantMoved
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.messaging.failures import ACK_POLICY
from plantkeeper.infrastructure.messaging.topics import EVENT_TOPICS, TELEMETRY_RAW
from plantkeeper.workers.consumers import (
    TELEMETRY_INGEST_OFFSET_RESET,
    register_consumers,
    register_telemetry_ingest,
    topics_for,
)

Subscriber = Callable[[KafkaMessage], Awaitable[None]]


@dataclass(frozen=True)
class Route:
    """One subscription the worker registered."""

    topic: str
    group_id: str
    title: str
    auto_offset_reset: str
    ack_policy: AckPolicy
    handler: Subscriber


class FakeBroker:
    """Records one :class:`Route` per subscription."""

    def __init__(self) -> None:
        self.routes: list[Route] = []

    def subscriber(
        self,
        topic: str,
        *,
        group_id: str,
        auto_offset_reset: str,
        title: str,
        description: str,
        ack_policy: AckPolicy,
    ) -> Callable[[Subscriber], Subscriber]:
        """Stand in for FastStream's decorator-returning ``subscriber``."""
        del description  # Only the title keys the generated channel.

        def decorator(handler: Subscriber) -> Subscriber:
            self.routes.append(
                Route(topic, group_id, title, auto_offset_reset, ack_policy, handler)
            )
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

    subscribed = {(route.topic, route.group_id) for route in broker.routes}
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

    titles = [route.title for route in broker.routes]
    assert len(titles) == len(set(titles))
    for route in broker.routes:
        assert route.title == f"{route.topic} to {route.group_id}"


def test_every_subscription_replays_from_the_beginning() -> None:
    """The ledger makes a replay safe, and a rebuilt consumer depends on it."""
    broker = FakeBroker()

    register_consumers(
        cast("KafkaBroker", broker),
        container=cast("AsyncContainer", None),
        settings=Settings(worker_consumer_group_prefix="test-worker"),
    )

    assert {route.auto_offset_reset for route in broker.routes} == {"earliest"}


def test_every_subscription_redelivers_a_failed_delivery() -> None:
    """The retry budget is only real if the transport offers the delivery again.

    FastStream's default acknowledgement policy commits the offset from aiokafka's
    own timer, so a handler that raised leaves a delivery that was neither handled
    nor redelivered — and a dead-letter copy that could never be retried.
    """
    broker = FakeBroker()

    register_consumers(
        cast("KafkaBroker", broker),
        container=cast("AsyncContainer", None),
        settings=Settings(worker_consumer_group_prefix="test-worker"),
    )
    register_telemetry_ingest(
        cast("KafkaBroker", broker), container=cast("AsyncContainer", None), settings=Settings()
    )

    assert {route.ack_policy for route in broker.routes} == {AckPolicy.NACK_ON_ERROR}
    assert ACK_POLICY is AckPolicy.NACK_ON_ERROR


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
    route = broker.routes[0]
    assert (route.topic, route.group_id) == (TELEMETRY_RAW, "test-telemetry-ingest")


def test_the_telemetry_ingress_starts_at_the_live_edge() -> None:
    """Raw telemetry has no ledger to replay against; the readings table is one."""
    broker = FakeBroker()

    register_telemetry_ingest(
        cast("KafkaBroker", broker),
        container=cast("AsyncContainer", None),
        settings=Settings(),
    )

    assert TELEMETRY_INGEST_OFFSET_RESET == "latest"
    assert broker.routes[0].auto_offset_reset == "latest"


def test_the_raw_topic_is_not_in_the_event_catalogue() -> None:
    """A topic that carries raw sensor JSON must not be taken for an event topic."""
    assert TELEMETRY_RAW not in set(EVENT_TOPICS.values())
