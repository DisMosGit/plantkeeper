"""The dead-letter port.

A delivery a consumer cannot handle is not dropped: it is copied to the
dead-letter topic with everything needed to put it back, and the consumer's claim
on it is recorded with the copy. See
``docs/adr/0011-consumer-failure-policy.md``.

The copy is the message *as it arrived* — body, headers and key — plus the three
facts the topic it lands on cannot express: which consumer group gave up on it,
which topic it came from, and why. An operator replaying it therefore needs
nothing from the original topic, and the provenance headers travel with it so a
moved-aside message is still traceable to the conversation that produced it.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict


class DeliveryDeadLetter(BaseModel):
    """One delivery a consumer gave up on, ready to be copied aside."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    consumer_group: str
    """Who gave up: the group whose ledger claim is committed with this copy."""

    original_topic: str
    """Where it came from, and where a replay puts it back."""

    partition_key: str
    """The Kafka key, so a replay keeps the aggregate's events in one partition.

    Empty only for a message that was published without a key.
    """

    body: str
    """The message body verbatim, so a replay re-publishes what was delivered."""

    headers: dict[str, str]
    """The delivery's own headers, provenance included."""

    error: str
    """The cause, as :func:`~plantkeeper.application.delivery.describe_failure` renders it."""


@runtime_checkable
class DeadLetterPublisher(Protocol):
    """Copies a delivery that exhausted its attempts to the dead-letter topic."""

    async def publish_moved_aside(self, delivery: DeliveryDeadLetter) -> None:
        """Copy ``delivery`` to the dead-letter topic.

        Raises when the broker did not acknowledge the copy. The caller then
        leaves the delivery unclaimed, so the transport offers it again instead
        of the message being dropped silently.
        """
        ...
