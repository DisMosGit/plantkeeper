"""SQLAlchemy implementations of the repository ports."""

from __future__ import annotations

from plantkeeper.infrastructure.persistence.repositories.care import (
    SqlAlchemyCareScheduleRepository,
)
from plantkeeper.infrastructure.persistence.repositories.catalog import (
    SqlAlchemySpeciesRepository,
)
from plantkeeper.infrastructure.persistence.repositories.garden import (
    SqlAlchemyHouseholdRepository,
    SqlAlchemyPlantRepository,
)
from plantkeeper.infrastructure.persistence.repositories.idempotency import (
    SqlAlchemyIdempotencyRepository,
)
from plantkeeper.infrastructure.persistence.repositories.journal import (
    SqlAlchemyJournalEntryRepository,
)
from plantkeeper.infrastructure.persistence.repositories.notifications import (
    SqlAlchemyNotificationRepository,
)
from plantkeeper.infrastructure.persistence.repositories.outbox import (
    SqlAlchemyOutboxRepository,
)
from plantkeeper.infrastructure.persistence.repositories.telemetry import (
    SqlAlchemySensorRepository,
)

__all__ = (
    "SqlAlchemyCareScheduleRepository",
    "SqlAlchemyHouseholdRepository",
    "SqlAlchemyIdempotencyRepository",
    "SqlAlchemyJournalEntryRepository",
    "SqlAlchemyNotificationRepository",
    "SqlAlchemyOutboxRepository",
    "SqlAlchemyPlantRepository",
    "SqlAlchemySensorRepository",
    "SqlAlchemySpeciesRepository",
)
