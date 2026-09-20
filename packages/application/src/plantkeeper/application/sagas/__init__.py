"""Sagas: the application layer's process managers and write-side consumers.

Two shapes live here, and ``docs/adr/0005-orchestration-vs-choreography.md``
explains why:

* :class:`~plantkeeper.application.sagas.base.Saga` — orchestration. A single
  process manager owns an ordered list of steps and compensates them backwards
  (``OnboardPlantSaga``, ``SpeciesSyncSaga``).
* :class:`~plantkeeper.application.sagas.consumer.Consumer` — choreography. Each
  event carries its own reaction and there is no central state
  (``AdaptiveWateringSaga``, ``MissedCareSaga``).

Both are started from Kafka by ``apps/workers``, and both are idempotent on
``(consumer_group, event_id)``.
"""

from __future__ import annotations

from plantkeeper.application.sagas.base import Saga
from plantkeeper.application.sagas.consumer import Consumer, consume_once
from plantkeeper.application.sagas.registry import (
    CONSUMER_TYPES,
    SAGA_TYPES,
    TRIGGER_TYPES,
    WORKER_CONSUMER_TYPES,
    build_saga_map,
    saga_type_named,
)

__all__ = (
    "CONSUMER_TYPES",
    "SAGA_TYPES",
    "TRIGGER_TYPES",
    "WORKER_CONSUMER_TYPES",
    "Consumer",
    "Saga",
    "build_saga_map",
    "consume_once",
    "saga_type_named",
)
