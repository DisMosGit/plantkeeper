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
from plantkeeper.infrastructure.messaging.topics import EVENT_TOPICS
from plantkeeper.workers.consumers import register_consumers, topics_for

Subscriber = Callable[[KafkaMessage], Awaitable[None]]


class FakeBroker:
    """Records ``(topic, group_id, handler)`` for every subscription."""

    def __init__(self) -> None:
        self.routes: list[tuple[str, str, str, Subscriber]] = []

    def subscriber(
        self, topic: str, *, group_id: str, auto_offset_reset: str
    ) -> Callable[[Subscriber], Subscriber]:
        """Stand in for FastStream's decorator-returning ``subscriber``."""

        def decorator(handler: Subscriber) -> Subscriber:
            self.routes.append((topic, group_id, auto_offset_reset, handler))
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

    subscribed = {(topic, group_id) for topic, group_id, _, _ in broker.routes}
    expected = {
        (EVENT_TOPICS[event_type], f"test-worker-{consumer_type.name}")
        for consumer_type in WORKER_CONSUMER_TYPES
        for event_type in consumer_type.handled_types
    }
    assert subscribed == expected


def test_every_subscription_replays_from_the_beginning() -> None:
    """The ledger makes a replay safe, and a rebuilt consumer depends on it."""
    broker = FakeBroker()

    register_consumers(
        cast("KafkaBroker", broker),
        container=cast("AsyncContainer", None),
        settings=Settings(worker_consumer_group_prefix="test-worker"),
    )

    assert {auto_offset_reset for _, _, auto_offset_reset, _ in broker.routes} == {"earliest"}
