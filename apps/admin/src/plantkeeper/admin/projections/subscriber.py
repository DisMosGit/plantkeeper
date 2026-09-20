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
* a **projection error** propagates, so FastStream rejects the delivery and the
  broker redelivers it. The ledger claim rolled back with the write, so a retry
  starts from nothing.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from faststream.kafka import KafkaBroker, KafkaMessage

from plantkeeper.admin.projections import ALL_PROJECTIONS
from plantkeeper.admin.projections.base import Projection, apply_event
from plantkeeper.infrastructure.messaging.decoding import decode_event

logger = logging.getLogger(__name__)

ProjectionSubscriber = Callable[[KafkaMessage], Awaitable[None]]


def register_projections(broker: KafkaBroker, *, prefix: str) -> None:
    """Attach every projection to its topics.

    Called before the broker starts, because FastStream refuses to add routes to a
    running broker. Each projection gets its own consumer group, so each one owns
    its offsets and its own idempotency domain.
    """
    for projection in ALL_PROJECTIONS:
        consumer_group = f"{prefix}-{projection.name}"
        for topic in projection.topics:
            broker.subscriber(
                topic,
                group_id=consumer_group,
                # A new consumer group replays its topic from the beginning; with
                # the ledger that is what makes a rebuilt read table possible.
                auto_offset_reset="earliest",
                # FastStream keys an AsyncAPI channel by the subscription's title
                # and otherwise falls back to the handler's function name, which
                # every handler here shares (``handle``). Without this, several
                # projections on one topic would collapse into one channel.
                title=f"{topic} to {consumer_group}",
                description=f"{projection.name} projection",
            )(build_handler(projection, consumer_group=consumer_group))


def build_handler(projection: Projection, *, consumer_group: str) -> ProjectionSubscriber:
    """Return the FastStream handler that feeds one projection."""

    async def handle(message: KafkaMessage) -> None:
        """Decode the delivery and project it, if it is this projection's."""
        event = decode_event(message)
        if event is None or not projection.handles(event):
            return
        projected = await apply_event(projection, event, consumer_group=consumer_group)
        if not projected:
            logger.debug(
                "event %s was already projected into %s",
                event.event_id,
                projection.name,
            )

    return handle
