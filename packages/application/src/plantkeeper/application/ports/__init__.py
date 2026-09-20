"""Ports: the interfaces the application layer needs from the outside world.

Every port is a ``typing.Protocol``, so the application layer never imports an
implementation and the infrastructure layer never has to subclass anything to
satisfy one (structural typing, checked by ``mypy --strict``).
"""

from __future__ import annotations

from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.event_publisher import EventPublisher
from plantkeeper.application.ports.idempotency import (
    IdempotencyRecord,
    IdempotencyRepository,
)
from plantkeeper.application.ports.outbox import OutboxMessage, OutboxRepository
from plantkeeper.application.ports.repositories import (
    CareScheduleRepository,
    HouseholdRepository,
    JournalEntryRepository,
    NotificationRepository,
    PlantRepository,
    SensorRepository,
    SpeciesRepository,
)
from plantkeeper.application.ports.unit_of_work import UnitOfWork

__all__ = (
    "CareScheduleRepository",
    "Clock",
    "EventPublisher",
    "HouseholdRepository",
    "IdempotencyRecord",
    "IdempotencyRepository",
    "JournalEntryRepository",
    "NotificationRepository",
    "OutboxMessage",
    "OutboxRepository",
    "PlantRepository",
    "SensorRepository",
    "SpeciesRepository",
    "UnitOfWork",
)
