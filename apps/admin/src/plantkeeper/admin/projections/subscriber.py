"""The Kafka side of the read side.

One subscriber per (projection, topic). The message is envelope-less — the body
is the event document and ``event_name``/``event_id`` travel in the headers — so
this module turns the header back into a type, the body into an event, and the
event into a projection call. The decoding itself lives in
``plantkeeper.infrastructure.messaging.decoding`` so the write side's sagas read
the same contract.

Failure policy, deliberately three-way:

* an **unknown ``event_name``** or a body that does not validate is logged and
  acknowledged. It is a contract violation, and blocking the partition behind it
  would stop every later event from being projected. The message stays in the
  topic and can be replayed once the producer is fixed.
* an event **this projection does not handle** is acknowledged silently: a topic
  carries every event of its context.
* a **projection error** runs under the platform's consumer failure policy
  (:mod:`plantkeeper.application.delivery`): a broken domain rule is copied aside
  at once, anything else is retried a bounded number of times with backoff, and a
  delivery past its budget is copied to the dead-letter topic with its consumer
  group, origin and cause in the headers while its ledger claim is recorded — so
  the partition moves on, and an operator can put the delivery back
  (``docs/adr/0011-consumer-failure-policy.md``).

The subscribers are registered with ``ack_policy=NACK_ON_ERROR`` for the same
reason the worker's are: FastStream's default commits the offset from aiokafka's
own timer, so a projection that raised would be neither handled nor redelivered.
Here the offset is committed once the handler returned and seeked back when it
raised, which is what makes the retry budget real rather than theoretical.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from asgiref.sync import sync_to_async
from faststream.kafka import KafkaBroker, KafkaMessage

from plantkeeper.admin.projections import ALL_PROJECTIONS
from plantkeeper.admin.projections.base import Projection, apply_event, record_move_aside
from plantkeeper.application.delivery import (
    FailurePolicy,
    describe_failure,
    run_with_failure_policy,
)
from plantkeeper.application.ports.dead_letter import DeadLetterPublisher
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.messaging.decoding import decode_event
from plantkeeper.infrastructure.messaging.failures import (
    ACK_POLICY,
    dead_letter_for,
    failure_policy,
)

logger = logging.getLogger(__name__)

ProjectionSubscriber = Callable[[KafkaMessage], Awaitable[None]]


def register_projections(
    broker: KafkaBroker,
    *,
    prefix: str,
    settings: Settings,
    publisher: DeadLetterPublisher,
) -> None:
    """Attach every projection to its topics.

    Called before the broker starts, because FastStream refuses to add routes to a
    running broker. Each projection gets its own consumer group, so each one owns
    its offsets and its own idempotency domain.
    """
    policy = failure_policy(settings)
    for projection in ALL_PROJECTIONS:
        consumer_group = f"{prefix}-{projection.name}"
        for topic in projection.topics:
            broker.subscriber(
                topic,
                group_id=consumer_group,
                # A new consumer group replays its topic from the beginning; with
                # the ledger that is what makes a rebuilt read table possible.
                auto_offset_reset="earliest",
                ack_policy=ACK_POLICY,
                # FastStream keys an AsyncAPI channel by the subscription's title
                # and otherwise falls back to the handler's function name, which
                # every handler here shares (``handle``). Without this, several
                # projections on one topic would collapse into one channel.
                title=f"{topic} to {consumer_group}",
                description=f"{projection.name} projection",
            )(
                build_handler(
                    projection,
                    consumer_group=consumer_group,
                    policy=policy,
                    publisher=publisher,
                )
            )


def build_handler(
    projection: Projection,
    *,
    consumer_group: str,
    policy: FailurePolicy,
    publisher: DeadLetterPublisher,
) -> ProjectionSubscriber:
    """Return the FastStream handler that feeds one projection."""

    async def handle(message: KafkaMessage) -> None:
        """Decode the delivery and project it, under the failure policy."""
        event = decode_event(message)
        if event is None or not projection.handles(event):
            return

        async def move_aside(error: Exception) -> None:
            """Copy the delivery to the dead-letter topic and claim it.

            Copy first, then claim: Django's ORM cannot join the broker's publish
            in one transaction the way the write side's unit of work does, and of
            the two orders this is the safe one — a copy without a claim is
            replayed twice at worst, while a claim without a copy drops the
            delivery outright. A refused copy raises, leaving the delivery
            unclaimed so the broker offers it again.
            """
            await publisher.publish_moved_aside(
                dead_letter_for(
                    message, consumer_group=consumer_group, error=describe_failure(error)
                )
            )
            await sync_to_async(record_move_aside, thread_sensitive=True)(
                event.event_id, consumer_group=consumer_group
            )

        projected = await run_with_failure_policy(
            lambda: apply_event(projection, event, consumer_group=consumer_group),
            move_aside=move_aside,
            policy=policy,
        )
        if not projected:
            logger.debug(
                "event %s was already projected into %s",
                event.event_id,
                projection.name,
            )

    return handle
