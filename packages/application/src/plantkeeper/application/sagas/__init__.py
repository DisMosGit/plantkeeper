"""Sagas: the application layer's process managers and write-side consumers.

Two shapes live here, and ``docs/adr/0005-orchestration-vs-choreography.md``
explains why:

* :class:`~plantkeeper.application.sagas.base.Saga` — orchestration. A single
  process manager owns an ordered list of steps and compensates them backwards
  (``OnboardPlantSaga``, ``SpeciesSyncSaga``).
* :class:`~plantkeeper.application.sagas.consumer.Consumer` — choreography. Each
  event carries its own reaction and there is no central state
  (``AdaptiveWateringSaga``, ``MissedCareSaga``, ``JournalEntryConsumer``).

Both are started from Kafka by ``apps/workers``, and both are idempotent on
``(consumer_group, event_id)``.

The registry (:mod:`plantkeeper.application.sagas.registry`) is deliberately *not*
re-exported here. It imports every consumer, and the journal's consumer imports
this package for its base class — re-exporting it would close that cycle, so the
worker and the tests import the registry by its own module name.
"""

from __future__ import annotations

from plantkeeper.application.sagas.base import Saga
from plantkeeper.application.sagas.consumer import Consumer, consume_once

__all__ = (
    "Consumer",
    "Saga",
    "consume_once",
)
