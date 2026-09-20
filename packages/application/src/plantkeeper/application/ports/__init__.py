"""Ports: the interfaces the application layer needs from the outside world.

Every port is a ``typing.Protocol``, so the application layer never imports an
implementation and the infrastructure layer never has to subclass anything to
satisfy one (structural typing, checked by ``mypy --strict``).
"""

from __future__ import annotations

from plantkeeper.application.ports.catalog import (
    SpeciesCache,
    SpeciesCatalog,
    SpeciesRecord,
    SpeciesSource,
)
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.event_publisher import EventPublisher
from plantkeeper.application.ports.event_store import (
    EventStoreRepository,
    JournalSnapshotRepository,
    StoredEvent,
)
from plantkeeper.application.ports.idempotency import (
    IdempotencyRecord,
    IdempotencyRepository,
)
from plantkeeper.application.ports.notifications import (
    NotificationChannel,
    NotificationChannelError,
    NotificationSubscription,
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
from plantkeeper.application.ports.sagas import (
    MissedCareState,
    MissedCareWindow,
    MissedCareWindowRepository,
    ProcessedEventRepository,
    SagaState,
    SagaStateRepository,
)
from plantkeeper.application.ports.unit_of_work import UnitOfWork

__all__ = (
    "CareScheduleRepository",
    "Clock",
    "EventPublisher",
    "EventStoreRepository",
    "HouseholdRepository",
    "IdempotencyRecord",
    "IdempotencyRepository",
    "JournalEntryRepository",
    "JournalSnapshotRepository",
    "MissedCareState",
    "MissedCareWindow",
    "MissedCareWindowRepository",
    "NotificationChannel",
    "NotificationChannelError",
    "NotificationRepository",
    "NotificationSubscription",
    "OutboxMessage",
    "OutboxRepository",
    "PlantRepository",
    "ProcessedEventRepository",
    "SagaState",
    "SagaStateRepository",
    "SensorRepository",
    "SpeciesCache",
    "SpeciesCatalog",
    "SpeciesRecord",
    "SpeciesRepository",
    "SpeciesSource",
    "StoredEvent",
    "UnitOfWork",
)
