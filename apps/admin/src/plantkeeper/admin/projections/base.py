"""What every projection has in common.

A projection is a Kafka consumer whose only job is to keep one read table in step
with the events a bounded context publishes. Three rules shape this module:

* **Idempotency belongs to the consumer, not to Kafka.** The relay delivers at
  least once (see ``docs/events.md``), so a projection writes its
  ``(consumer_group, event_id)`` row *before* it projects anything, and a second
  delivery finds the row and stops. That is the contract ``AGENTS.md`` requires.
* **The ledger and the projection commit together.** A crash between the two
  would either lose the event or apply it twice; one transaction makes the second
  delivery a no-op instead.
* **Django's ORM is used synchronously, on purpose.** Django 6.1 has no async
  ``transaction.atomic``, and every projection is a short, sequential write. The
  subscriber hands :meth:`Projection.apply` to ``sync_to_async``, so it runs on a
  thread with a real transaction instead of one implicit transaction per
  statement.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, ClassVar

from asgiref.sync import sync_to_async
from django.db import transaction

from plantkeeper.admin.read_models.models import ProcessedEvent
from plantkeeper.domain.base import DomainEvent

ProjectionHandler = Callable[[Any, Any], None]
"""A projection's handler for one event type, as stored in ``handlers``.

``Any`` is unavoidable at exactly this seam, and nowhere else in the read side: a
handler narrows its argument to one concrete event (``PlantAdded``,
``WateringCompleted``, …) and the base class looks it up by that class at
runtime. A mapping typed ``Callable[[Projection, object], None]`` would reject
the narrower handlers — callable parameters are contravariant — and a protocol
per projection would only restate the dispatch. Every handler is annotated with
its concrete event type, so the code that does the work stays checked.
"""


class Projection:
    """One read model's writer, behind one consumer group."""

    name: ClassVar[str]
    """The group's suffix: ``<prefix>-<name>`` is the full consumer group."""

    topics: ClassVar[tuple[str, ...]]
    """The topic(s) this projection consumes."""

    handlers: ClassVar[Mapping[type[DomainEvent], ProjectionHandler]] = {}
    """Which event class each handler answers."""

    def handles(self, event: DomainEvent) -> bool:
        """Whether this projection has a handler for ``event``'s type.

        A topic carries every event of its context, so most deliveries are for
        another consumer's events; those are dropped without touching the ledger.
        """
        return type(event) in self.handlers

    def apply(self, event: DomainEvent, *, consumer_group: str) -> bool:
        """Project ``event`` once for ``consumer_group``.

        Returns ``True`` when the event was projected, ``False`` when it had
        already been seen or is not this projection's to handle. The claim on the
        event and the projection's own writes belong to the same transaction, so
        a failure releases the claim as well and the retry starts from nothing.
        """
        handler = self.handlers.get(type(event))
        if handler is None:
            return False
        with transaction.atomic():
            _, created = ProcessedEvent.objects.get_or_create(
                consumer_group=consumer_group,
                event_id=event.event_id,
            )
            if not created:
                return False
            handler(self, event)
            return True


async def apply_event(projection: Projection, event: DomainEvent, *, consumer_group: str) -> bool:
    """Run :meth:`Projection.apply` on Django's thread executor.

    This is the single path production and tests share: the consumer awaits this
    function, and a test can drive a projection without a broker.
    """
    return await sync_to_async(projection.apply, thread_sensitive=True)(
        event, consumer_group=consumer_group
    )
