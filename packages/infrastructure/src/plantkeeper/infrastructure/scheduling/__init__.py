"""Background jobs the worker runs next to the outbox relay.

Each one is a :class:`~plantkeeper.infrastructure.scheduling.job.BackgroundJob`:
a loop with a ``run`` and a ``stop`` that the worker's signal handler can call.
They are constructed in ``apps/workers`` where the app container is available,
rather than provided by Dishka, because each one needs to open request scopes of
its own — a job that shared the worker's container would keep one session alive
for the lifetime of the process.
"""

from __future__ import annotations

from plantkeeper.infrastructure.scheduling.job import BackgroundJob
from plantkeeper.infrastructure.scheduling.missed_care import MissedCareScheduler
from plantkeeper.infrastructure.scheduling.saga_recovery import SagaRecoveryJob
from plantkeeper.infrastructure.scheduling.species_sync import SpeciesSyncScheduler

__all__ = (
    "BackgroundJob",
    "MissedCareScheduler",
    "SagaRecoveryJob",
    "SpeciesSyncScheduler",
)
