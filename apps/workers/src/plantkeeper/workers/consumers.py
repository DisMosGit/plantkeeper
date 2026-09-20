"""Kafka subscriptions for the write side's consumers.

The read side has its own registration module
(``plantkeeper.admin.projections.subscriber``) because its consumers talk to
Django. This one registers the write side's: every saga consumer, each with its
own group id and its own ``(consumer_group, event_id)`` ledger domain.

Registration must happen **before** ``broker.start()``: FastStream refuses to add
routes to a running broker.

Each delivery opens a Dishka request scope. That scope is the unit of work the
consumer's handler and — for a trigger — the saga's step handlers all share, so
the ledger claim, the step writes and the outbox rows travel in one transaction
where they must, and in the saga's own checkpointed transactions where a saga
requires that instead.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.requests.map import SagaMap
from cqrs.saga.storage.protocol import ISagaStorage
from dishka import AsyncContainer
from faststream.kafka import KafkaBroker, KafkaMessage

from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.application.sagas.registry import CONSUMER_TYPES, TRIGGER_TYPES
from plantkeeper.domain.base import DomainEvent
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.cqrs import DishkaCQRSContainer
from plantkeeper.infrastructure.messaging.decoding import decode_event
from plantkeeper.infrastructure.messaging.topics import EVENT_TOPICS

logger = logging.getLogger(__name__)

ConsumerSubscriber = Callable[[KafkaMessage], Awaitable[None]]


def topics_for(events: tuple[type[DomainEvent], ...]) -> tuple[str, ...]:
    """Return the distinct topics the events travel on, without duplicates.

    The topic layout lives in the infrastructure layer, which is why the
    application's consumers declare the events they handle and the worker derives
    the subscriptions.
    """
    return tuple(dict.fromkeys(EVENT_TOPICS[event] for event in events))


def register_consumers(
    broker: KafkaBroker, *, container: AsyncContainer, settings: Settings
) -> None:
    """Attach every write-side consumer to its topics."""
    for consumer_type in CONSUMER_TYPES + TRIGGER_TYPES:
        consumer_group = f"{settings.worker_consumer_group_prefix}-{consumer_type.name}"
        for topic in topics_for(consumer_type.handled_types):
            broker.subscriber(
                topic,
                group_id=consumer_group,
                # A new consumer group replays its topic from the beginning; the
                # ledger makes that a safe way to rebuild a consumer's state.
                auto_offset_reset="earliest",
            )(
                build_consumer_handler(
                    consumer_type, container=container, consumer_group=consumer_group
                )
            )


def build_consumer_handler(
    consumer_type: type[Consumer], *, container: AsyncContainer, consumer_group: str
) -> ConsumerSubscriber:
    """Return the FastStream handler that feeds one consumer group."""

    async def handle(message: KafkaMessage) -> None:
        """Decode the delivery and consume it, if it is this consumer's."""
        event = decode_event(message)
        if event is None or type(event) not in consumer_type.handled_types:
            # A topic carries every event of its context; filtering before the
            # scope is opened keeps the deliveries this group ignores from taking
            # a database session at all.
            return
        async with container() as request_container:
            consumer = await request_container.get(consumer_type)
            dispatcher = await build_saga_dispatcher(request_container)
            if not await consumer.consume(
                event, consumer_group=consumer_group, dispatcher=dispatcher
            ):
                logger.debug("event %s was already consumed by %s", event.event_id, consumer_group)

    return handle


async def build_saga_dispatcher(request_container: AsyncContainer) -> SagaDispatcher:
    """Build the cqrs dispatcher over the delivery's own request scope.

    The dispatcher resolves the saga and its step handlers from this container, so
    they share the scope's unit of work; the storage keeps its own session, which
    is what makes each step's commit a durable checkpoint.
    """
    saga_map = await request_container.get(SagaMap)
    storage = await request_container.get(ISagaStorage)
    return SagaDispatcher(saga_map, DishkaCQRSContainer(request_container), storage)
