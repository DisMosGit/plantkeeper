"""Aggregate tracking for the unit of work.

The unit of work has to know which aggregates raised events before it can append
them to the outbox. Repositories register an aggregate when it is added or saved,
and :class:`AggregateTracker` collects the recorded events at commit time.

Only ``add`` and ``save`` register an aggregate: an aggregate that was merely
read and never written has nothing to publish, and registering reads would let a
forgotten ``save`` publish an event for a change that was never persisted.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from plantkeeper.domain.base import DomainEvent


@runtime_checkable
class EventRecording(Protocol):
    """What the tracker needs from an aggregate.

    Structural rather than ``AggregateRoot[...]``: an aggregate is generic over
    its identifier, and a list of them cannot be written without naming one
    identifier type.
    """

    def collect_events(self) -> list[DomainEvent]:
        """Return the recorded events and forget them."""
        ...


class AggregateTracker:
    """The aggregates a transaction has touched, in touch order."""

    def __init__(self) -> None:
        self._aggregates: list[EventRecording] = []

    def track(self, aggregate: EventRecording) -> None:
        """Register an aggregate, ignoring a repeated registration."""
        if not any(aggregate is tracked for tracked in self._aggregates):
            self._aggregates.append(aggregate)

    def collect_events(self) -> list[DomainEvent]:
        """Drain every tracked aggregate's events, in the order it was touched.

        ``collect_events`` drains rather than copies (see
        ``plantkeeper.domain.base.AggregateRoot``), so a second call on the same
        unit of work cannot publish the same event twice.
        """
        events = [event for aggregate in self._aggregates for event in aggregate.collect_events()]
        self._aggregates.clear()
        return events
