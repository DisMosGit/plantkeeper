"""The write-side Kafka consumer.

The read side has ``plantkeeper.admin.projections.base.Projection``: a consumer
whose only job is to keep one read table in step. This is its write-side twin,
and it follows the same two rules:

* **Idempotency belongs to the consumer.** ``AGENTS.md`` requires every Kafka
  consumer to be idempotent on ``(consumer_group, event_id)``, so the delivery is
  claimed in ``write_shared.processed_events`` before any work happens. A second
  delivery of the same event in the same group stops at the claim.
* **The claim and the handler commit together.** One transaction makes a crash
  after the claim a no-op on redelivery, and a handler failure releases the claim
  so the retry starts from nothing.

The difference from a projection is what the handler does: it may publish
follow-up events to the outbox, and for the orchestration sagas it dispatches a
process manager (see :mod:`plantkeeper.application.sagas.base`).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import ClassVar
from uuid import UUID

from cqrs.dispatcher.saga import SagaDispatcher

from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.domain.base import DomainEvent


async def consume_once(
    unit_of_work: UnitOfWork,
    *,
    consumer_group: str,
    event_id: UUID,
    handler: Callable[[], Awaitable[None]],
) -> bool:
    """Claim one delivery and run ``handler`` with it, in one transaction.

    Shared by the choreography consumers and by the saga triggers: both need
    exactly this, and a second implementation would be a second place for the
    claim to drift out of the handler's transaction.
    """
    async with unit_of_work:
        if not await unit_of_work.processed_events.claim(consumer_group, event_id):
            return False
        await handler()
        await unit_of_work.commit()
    return True


class Consumer(ABC):
    """One consumer group's worth of event handling."""

    name: ClassVar[str]
    """The group's suffix: ``<prefix>-<name>`` is the full consumer group."""

    handled_types: ClassVar[tuple[type[DomainEvent], ...]]
    """The events this consumer reacts to.

    The Kafka topics are derived from these by the worker that registers the
    subscription, because the topic layout is an infrastructure concern and the
    application layer must not import it.
    """

    def __init__(self, unit_of_work: UnitOfWork) -> None:
        self._unit_of_work = unit_of_work

    @property
    def unit_of_work(self) -> UnitOfWork:
        """The transaction :meth:`handle` is running inside."""
        return self._unit_of_work

    def handles(self, event: DomainEvent) -> bool:
        """Whether this consumer reacts to ``event``'s type.

        A topic carries every event of its context, so most deliveries are for
        another consumer and are acknowledged without touching the ledger.
        """
        return type(event) in self.handled_types

    @abstractmethod
    async def handle(self, event: DomainEvent, dispatcher: SagaDispatcher) -> None:
        """Do the work of one delivery.

        The unit of work is already open and the delivery is already claimed;
        the transaction is committed by :meth:`consume`. ``dispatcher`` is for the
        consumers that start an orchestration saga; a choreography consumer
        ignores it.
        """
        ...

    async def consume(
        self, event: DomainEvent, *, consumer_group: str, dispatcher: SagaDispatcher
    ) -> bool:
        """Consume ``event`` once for ``consumer_group``.

        Returns ``True`` when the handler ran, ``False`` when the event was not
        this consumer's or had already been seen.
        """
        if not self.handles(event):
            return False
        return await consume_once(
            self._unit_of_work,
            consumer_group=consumer_group,
            event_id=event.event_id,
            handler=lambda: self.handle(event, dispatcher),
        )
