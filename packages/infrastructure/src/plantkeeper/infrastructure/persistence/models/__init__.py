"""ORM models, imported for their side effect on ``Base.metadata``.

Alembic imports this module to discover every table; a mapper that imports one
model directly does not need it. Keeping the list here means a new context's
tables appear in migrations without touching ``alembic/env.py``.

There is no Identity model yet: nothing writes a user in Phases 0-2, and the
``write_identity`` schema (already created by ``docker/postgres/init`` and by the
first migration) stays empty until a user flow needs it.
"""

from __future__ import annotations

from plantkeeper.infrastructure.persistence.models.care import CareScheduleModel
from plantkeeper.infrastructure.persistence.models.catalog import SpeciesModel
from plantkeeper.infrastructure.persistence.models.garden import HouseholdModel, PlantModel
from plantkeeper.infrastructure.persistence.models.journal import JournalEntryModel
from plantkeeper.infrastructure.persistence.models.notifications import NotificationModel
from plantkeeper.infrastructure.persistence.models.shared import (
    IdempotencyKeyModel,
    OutboxModel,
)
from plantkeeper.infrastructure.persistence.models.telemetry import SensorModel

__all__ = (
    "CareScheduleModel",
    "HouseholdModel",
    "IdempotencyKeyModel",
    "JournalEntryModel",
    "NotificationModel",
    "OutboxModel",
    "PlantModel",
    "SensorModel",
    "SpeciesModel",
)
