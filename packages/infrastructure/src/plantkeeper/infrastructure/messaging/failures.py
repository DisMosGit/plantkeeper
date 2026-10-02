"""The infrastructure side of the consumer failure policy.

:mod:`plantkeeper.application.delivery` holds the policy itself; two things have to
happen before a subscriber can use it, and both are infrastructure's business:

* the configured budget and backoff become a
  :class:`~plantkeeper.application.delivery.FailurePolicy` (:func:`failure_policy`);
* a Kafka delivery becomes the
  :class:`~plantkeeper.application.ports.dead_letter.DeliveryDeadLetter` the
  dead-letter publisher copies (:func:`dead_letter_for`).

The acknowledgement policy lives here too. It is the half of the policy that
FastStream owns, and the default is the wrong one for this platform: ``ACK_FIRST``
lets aiokafka's own timer commit the offset, so a handler that fails — or a
process that dies mid-handler — leaves the delivery neither handled nor
redelivered, and the ledger claim it rolled back is the only trace.
``NACK_ON_ERROR`` commits the offset once the handler returned and seeks back to
the delivery when it raised, which is what makes "retried, then moved aside"
observable instead of theoretical.
"""

from __future__ import annotations

from typing import Any, Final

from aiokafka import ConsumerRecord
from faststream.kafka import KafkaMessage
from faststream.middlewares import AckPolicy

from plantkeeper.application.delivery import FailurePolicy
from plantkeeper.application.ports.dead_letter import DeliveryDeadLetter
from plantkeeper.infrastructure.config import Settings

ACK_POLICY: Final = AckPolicy.NACK_ON_ERROR
"""Commit after a handled delivery, redeliver one whose handler raised.

Every subscriber in the platform sets it. Without it the retry budget below would
only ever run inside one process, and a crash between the handler and the commit
would lose the delivery outright.
"""


def failure_policy(settings: Settings) -> FailurePolicy:
    """Return the consumer retry budget the environment configures."""
    return FailurePolicy(
        max_attempts=settings.consumer_max_attempts,
        initial_wait_seconds=settings.consumer_retry_initial_wait_seconds,
        max_wait_seconds=settings.consumer_retry_max_wait_seconds,
    )


def dead_letter_for(
    message: KafkaMessage,
    *,
    consumer_group: str,
    error: str,
) -> DeliveryDeadLetter:
    """Describe a delivery that could not be handled, as the copy to store.

    The body and the headers travel verbatim, so what lands on the dead-letter
    topic is the message that was delivered rather than a re-rendering of it, and
    a replay puts back exactly what the consumer could not handle.
    """
    record = _record_of(message)
    return DeliveryDeadLetter(
        consumer_group=consumer_group,
        original_topic=record.topic,
        partition_key=_key_text(record.key),
        body=_body_text(message.body),
        headers={name: str(value) for name, value in message.headers.items()},
        error=error,
    )


def _record_of(message: KafkaMessage) -> ConsumerRecord[Any, Any]:
    """Return the Kafka record behind a delivery.

    Only batch subscribers get a tuple of records, and no subscriber in this
    platform uses batch mode: one delivery is one record.
    """
    record = message.raw_message
    if isinstance(record, tuple):
        record = record[0]
    return record


def _key_text(key: Any) -> str:
    """Return a record's key as text, or an empty key when it has none.

    ``Any`` is the broker's own typing: a Kafka key is whatever bytes the producer
    sent, and the two shapes that matter are handled here.
    """
    if key is None:
        return ""
    if isinstance(key, bytes):
        return key.decode("utf-8", errors="replace")
    return str(key)


def _body_text(body: Any) -> str:
    """Return a delivery's body as text, as the dead-letter copy stores it."""
    if isinstance(body, bytes):
        return body.decode("utf-8", errors="replace")
    return str(body)
