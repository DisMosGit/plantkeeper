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

Each delivery also **binds its provenance** for the span of the scope. Everything
the handler raises therefore reaches the outbox stamped with the delivery's
conversation and as caused by the delivered event, and no consumer signature has
to carry it (see :mod:`plantkeeper.application.provenance`).

Each delivery runs under the platform's **failure policy**
(:mod:`plantkeeper.application.delivery`): a broken domain rule is copied aside at
once, anything else is retried a bounded number of times with backoff. The copy
and the ledger claim commit together, so the partition moves on and the delivery
is neither retried forever nor handled twice
(``docs/adr/0011-consumer-failure-policy.md``).
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

from plantkeeper.application.delivery import (
    describe_failure,
    run_with_failure_policy,
)
from plantkeeper.application.ports.dead_letter import DeadLetterPublisher
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.provenance import async_provenance_scope, request_context
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.application.sagas.registry import CONSUMER_TYPES, TRIGGER_TYPES
from plantkeeper.application.telemetry.ingest import TelemetryIngestConsumer
from plantkeeper.domain.base import DomainEvent
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.di.cqrs import DishkaCQRSContainer
from plantkeeper.infrastructure.messaging.decoding import decode_message, read_header
from plantkeeper.infrastructure.messaging.failures import (
    ACK_POLICY,
    dead_letter_for,
    failure_policy,
)
from plantkeeper.infrastructure.messaging.topics import (
    EVENT_TOPICS,
    HEADER_RAISED_BY,
    HEADER_TRACEPARENT,
)

logger = logging.getLogger(__name__)

TELEMETRY_RAISER: Final = "service:iot-simulator"
"""Who a raw telemetry delivery is attributed to when it names nobody.

The raw topic's producer is a simulated device, so the events its readings turn
into are attributed to the device rather than to ``system:unknown``.
"""

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
                # The offset is committed once the handler returned and seeked back
                # when it raised, so a delivery the policy could not handle really
                # is offered again (``plantkeeper.infrastructure.messaging.failures``).
                ack_policy=ACK_POLICY,
                # FastStream derives an AsyncAPI channel's key from the title and
                # falls back to the handler's function name, which every handler
                # here shares (``handle``). Without a title, several groups on one
                # topic would collapse into a single ``<topic>:Handle`` channel and
                # the document would silently drop all but the last of them.
                title=channel_title(topic, consumer_group),
                description=consumer_description(consumer_type),
            )(
                build_consumer_handler(
                    consumer_type,
                    container=container,
                    consumer_group=consumer_group,
                    settings=settings,
                )
            )


def build_consumer_handler(
    consumer_type: type[Consumer],
    *,
    container: AsyncContainer,
    consumer_group: str,
    settings: Settings,
) -> ConsumerSubscriber:
    """Return the FastStream handler that feeds one consumer group."""

    async def handle(message: KafkaMessage) -> None:
        """Decode the delivery and consume it, under the failure policy."""
        delivered = decode_message(message)
        if delivered is None or type(delivered.event) not in consumer_type.handled_types:
            # A topic carries every event of its context; filtering before the
            # scope is opened keeps the deliveries this group ignores from taking
            # a database session at all.
            return
        async with async_provenance_scope(delivered.context), container() as request_container:
            consumer = await request_container.get(consumer_type)
            dispatcher = await build_saga_dispatcher(request_container)
            unit_of_work = await request_container.get(UnitOfWork)
            publisher = await request_container.get(DeadLetterPublisher)

            async def move_aside(error: Exception) -> None:
                """Copy the delivery to the dead-letter topic and claim it.

                One transaction: the claim on the delivery and the copy of it are
                committed together, so the group never handles it twice. A copy the
                broker refuses raises, which leaves the claim unwritten and lets the
                transport offer the delivery again — the one outcome that must not
                be silent.
                """
                copy = dead_letter_for(
                    message, consumer_group=consumer_group, error=describe_failure(error)
                )
                async with unit_of_work:
                    if not await unit_of_work.processed_events.claim(
                        consumer_group, delivered.event.event_id
                    ):
                        # Already claimed: an orchestration trigger whose saga
                        # failed commits the failure — and the claim with it — on
                        # purpose, because that failure is the saga's to retry
                        # within its own budget (``docs/sagas.md``).
                        return
                    await publisher.publish_moved_aside(copy)
                    await unit_of_work.commit()

            await run_with_failure_policy(
                lambda: consumer.consume(
                    delivered.event, consumer_group=consumer_group, dispatcher=dispatcher
                ),
                move_aside=move_aside,
                policy=failure_policy(settings),
            )

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
        ack_policy=ACK_POLICY,
        title=channel_title(settings.telemetry_raw_topic, settings.telemetry_ingest_consumer_group),
        description="TelemetryIngestConsumer validates a raw measurement and stores it",
    )(build_telemetry_ingest_handler(container=container, settings=settings))


def build_telemetry_ingest_handler(
    *, container: AsyncContainer, settings: Settings
) -> ConsumerSubscriber:
    """Return the FastStream handler that feeds one raw delivery to the ingress."""

    async def handle(message: KafkaMessage) -> None:
        """Hand the raw body to the ingress under the failure policy.

        The raw topic carries no provenance headers — the simulator is a device,
        not a context — so the ingress starts the conversation itself and names
        the device as the raiser. The reading's own ``recorded_at``, not this
        instant, is what the events carry as their occurrence.

        The ingress keeps no ``processed_events`` ledger (``AGENTS.md``): it is
        idempotent on the reading's own ``(sensor_id, recorded_at)`` key instead.
        Its dead-letter copy therefore records no claim — there is none to
        record — and a replay is absorbed by that same key.
        """
        # ``StreamMessage`` assigns ``body`` in ``__init__`` without annotating it,
        # so the declared type is unknown; a Kafka delivery's body is the bytes
        # the producer sent.
        payload = cast("bytes", message.body)
        context = request_context(
            raised_by=read_header(message, HEADER_RAISED_BY) or TELEMETRY_RAISER,
            traceparent=read_header(message, HEADER_TRACEPARENT),
        )
        async with async_provenance_scope(context), container() as request_container:
            consumer = await request_container.get(TelemetryIngestConsumer)
            publisher = await request_container.get(DeadLetterPublisher)

            async def move_aside(error: Exception) -> None:
                """Copy the raw delivery aside for an operator to replay."""
                await publisher.publish_moved_aside(
                    dead_letter_for(
                        message,
                        consumer_group=settings.telemetry_ingest_consumer_group,
                        error=describe_failure(error),
                    )
                )

            stored = await run_with_failure_policy(
                lambda: consumer.ingest(payload),
                move_aside=move_aside,
                policy=failure_policy(settings),
            )
            if not stored:
                # The ingress answered "nothing to store": the reading was already
                # there, or it was dropped (unparseable, or from a sensor nobody
                # registered — both already logged with their own reason).
                logger.debug("telemetry delivery was dropped rather than stored")

    return handle
