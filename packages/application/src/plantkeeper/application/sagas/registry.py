"""The saga/consumer registry.

Every saga and consumer is listed here exactly once, and the worker builds its
Kafka subscriptions from these lists. The lists are explicit rather than
discovered by import scanning: a consumer that nobody registered is a silent
hole, and the registry is the one place a test can assert the whole set.
"""

from __future__ import annotations

from cqrs.requests.map import SagaMap

from plantkeeper.application.journal.consumer import JournalEntryConsumer
from plantkeeper.application.notifications.consumer import NotificationConsumer
from plantkeeper.application.notifications.pusher import NotificationPusher
from plantkeeper.application.sagas.adaptive_watering import AdaptiveWateringSaga
from plantkeeper.application.sagas.base import Saga
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.application.sagas.missed_care import MissedCareSaga
from plantkeeper.application.sagas.onboard import OnboardPlantSaga, OnboardPlantTrigger
from plantkeeper.application.sagas.species_sync import SpeciesSyncSaga, SpeciesSyncTrigger

CONSUMER_TYPES: tuple[type[Consumer], ...] = (
    AdaptiveWateringSaga,
    MissedCareSaga,
    JournalEntryConsumer,
    NotificationConsumer,
    NotificationPusher,
)
"""Choreography consumers: they react on their own, with no process manager.

The journal recorder and the two notification consumers are not sagas: the
recorder turns ``WateringCompleted`` into a journal entry, ``NotificationConsumer``
turns care and telemetry facts into reminders, and ``NotificationPusher`` wakes
the households a long poll is waiting for. Like the sagas, each owns a consumer
group and a ledger domain of its own.
"""

TRIGGER_TYPES: tuple[type[Consumer], ...] = (
    OnboardPlantTrigger,
    SpeciesSyncTrigger,
)
"""Consumers whose handler dispatches an orchestration saga."""

SAGA_TYPES: tuple[type[Saga], ...] = (
    OnboardPlantSaga,
    SpeciesSyncSaga,
)
"""The orchestration sagas, one per saga context."""

WORKER_CONSUMER_TYPES: tuple[type[Consumer], ...] = CONSUMER_TYPES + TRIGGER_TYPES
"""Everything the worker subscribes to."""


def build_saga_map() -> SagaMap:
    """Bind each saga context type to its saga, for the cqrs dispatcher."""
    saga_map = SagaMap()
    for saga_type in SAGA_TYPES:
        saga_map.bind(saga_type.context_type, saga_type)
    return saga_map


def saga_type_named(name: str) -> type[Saga] | None:
    """Return the saga class called ``name``, or ``None``.

    ``write_shared.saga_state`` stores the class name rather than the class, so
    this is how a reader of that table — or of :class:`SagaState`, which carries the
    same string — gets back to the saga that owns a row.
    """
    for saga_type in SAGA_TYPES:
        if saga_type.__name__ == name:
            return saga_type
    return None
