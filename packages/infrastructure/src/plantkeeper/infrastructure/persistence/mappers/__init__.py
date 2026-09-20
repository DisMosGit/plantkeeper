"""Aggregate <-> row mappers.

One module per bounded context, and a pair of functions in each: ``*_to_domain``
rebuilds an aggregate through its constructor (which re-checks the invariants, so
a corrupt row cannot enter the domain quietly) and ``*_to_model`` builds the row.
The mapping lives only here, so a column rename touches one module.
"""

from __future__ import annotations

from plantkeeper.infrastructure.persistence.mappers.care import (
    care_schedule_to_domain,
    care_schedule_to_model,
)
from plantkeeper.infrastructure.persistence.mappers.catalog import (
    species_to_domain,
    species_to_model,
)
from plantkeeper.infrastructure.persistence.mappers.garden import (
    household_to_domain,
    household_to_model,
    plant_to_domain,
    plant_to_model,
)
from plantkeeper.infrastructure.persistence.mappers.journal import (
    journal_entry_to_domain,
    journal_entry_to_model,
)
from plantkeeper.infrastructure.persistence.mappers.notifications import (
    notification_to_domain,
    notification_to_model,
)
from plantkeeper.infrastructure.persistence.mappers.outbox import (
    idempotency_to_domain,
    idempotency_to_model,
    outbox_message_from_model,
    outbox_model_from_event,
)
from plantkeeper.infrastructure.persistence.mappers.telemetry import (
    sensor_to_domain,
    sensor_to_model,
)

__all__ = (
    "care_schedule_to_domain",
    "care_schedule_to_model",
    "household_to_domain",
    "household_to_model",
    "idempotency_to_domain",
    "idempotency_to_model",
    "journal_entry_to_domain",
    "journal_entry_to_model",
    "notification_to_domain",
    "notification_to_model",
    "outbox_message_from_model",
    "outbox_model_from_event",
    "plant_to_domain",
    "plant_to_model",
    "sensor_to_domain",
    "sensor_to_model",
    "species_to_domain",
    "species_to_model",
)
