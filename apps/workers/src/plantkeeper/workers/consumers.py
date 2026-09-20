"""Kafka subscriptions for the write side's consumers.

The read side has its own registration module
(``plantkeeper.admin.projections.subscriber``) because its consumers talk to
Django. This one registers the write side's: every saga consumer, each with its
own group id and its own ``(consumer_group, event_id)`` ledger domain, plus the
telemetry ingress, which is the one subscriber that does not read a domain event
(see :func:`register_telemetry_ingest`).

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
from typing import Final, cast

from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.requests.map import SagaMap
from cqrs.saga.storage.protocol import ISagaStorage
from dishka import AsyncContainer
from faststream.kafka import KafkaBroker, KafkaMessage

from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.application.sagas.registry import CONSUMER_TYPES, TRIGGER_TYPES
from plantkeeper.application.telemetry.ingest import TelemetryIngestConsumer
from plantkeeper.domain.base import DomainEvent
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.cqrs import DishkaCQRSContainer
from plantkeeper.infrastructure.messaging.decoding import decode_event
from plantkeeper.infrastructure.messaging.topics import EVENT_TOPICS

logger = logging.getLogger(__name__)

ConsumerSubscriber = Callable[[KafkaMessage], Awaitable[None]]

TELEMETRY_INGEST_OFFSET_RESET: Final = "latest"
"""Where the ingress starts when its group has no committed offset.

``latest``, unlike the sagas' ``earliest``: a saga replays from the beginning
because its ledger makes that a safe rebuild, while raw telemetry has no ledger —
the readings table is its ledger — and replaying an old log would re-insert rows
nobody asked for. A fresh group therefore starts at the live edge, which is what
an operator running the simulator expects to see.
"""


def topics_for(events: tuple[type[DomainEvent], ...]) -> tuple[str, ...]:
    """Return the distinct topics the events travel on, without duplicates.

    The topic layout lives in the infrastructure layer, which is why the
    application's consumers declare the events they handle and the worker derives
    the subscriptions.
    """
    return tuple(dict.fromkeys(EVENT_TOPICS[event] for event in events))


def channel_title(topic: str, consumer_group: str) -> str:
    """Return the unique AsyncAPI title of one ``(topic, group)`` subscription.

    The title is what keeps the generated document honest: one Kafka topic is
    consumed by several groups, and AsyncAPI keys a channel by this string.
    """
    return f"{topic} to {consumer_group}"


def consumer_description(consumer_type: type[Consumer]) -> str:
    """Return the AsyncAPI description of one consumer's subscription."""
    handled = ", ".join(event.__name__ for event in consumer_type.handled_types)
    return f"{consumer_type.__name__} handles {handled}"


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
                # FastStream derives an AsyncAPI channel's key from the title and
                # falls back to the handler's function name, which every handler
                # here shares (``handle``). Without a title, several groups on one
                # topic would collapse into a single ``<topic>:Handle`` channel and
                # the document would silently drop all but the last of them.
                title=channel_title(topic, consumer_group),
                description=consumer_description(consumer_type),
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


def register_telemetry_ingest(
    broker: KafkaBroker, *, container: AsyncContainer, settings: Settings
) -> None:
    """Attach the telemetry ingress to the raw topic.

    Separate from :func:`register_consumers` because the two subscriptions are
    derived from different things: a saga names the *events* it handles and the
    worker looks their topics up, while this consumer reads a topic that is not in
    the event catalogue at all. Keeping them apart is what stops the next reader
    from concluding that ``telemetry.raw`` is a domain-event topic.
    """
    broker.subscriber(
        settings.telemetry_raw_topic,
        group_id=settings.telemetry_ingest_consumer_group,
        auto_offset_reset=TELEMETRY_INGEST_OFFSET_RESET,
        title=channel_title(settings.telemetry_raw_topic, settings.telemetry_ingest_consumer_group),
        description="TelemetryIngestConsumer validates a raw measurement and stores it",
    )(build_telemetry_ingest_handler(container=container))


def build_telemetry_ingest_handler(*, container: AsyncContainer) -> ConsumerSubscriber:
    """Return the FastStream handler that feeds one raw delivery to the ingress."""

    async def handle(message: KafkaMessage) -> None:
        """Hand the raw body to the ingress inside its own request scope."""
        # ``StreamMessage`` assigns ``body`` in ``__init__`` without annotating it,
        # so the declared type is unknown; a Kafka delivery's body is the bytes
        # the producer sent.
        payload = cast("bytes", message.body)
        async with container() as request_container:
            consumer = await request_container.get(TelemetryIngestConsumer)
            stored = await consumer.ingest(payload)
            if not stored:
                logger.debug("telemetry delivery was dropped rather than stored")

    return handle
