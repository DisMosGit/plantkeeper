"""Dishka providers for the write side.

Scopes follow the lifetimes of the things themselves:

* ``APP`` — settings, the clock, the engine, the session factory, the Kafka
  broker, the outbox relay and the request map: one per process;
* ``REQUEST`` — the session, the unit of work, the repositories and the
  handlers: one per HTTP request or message, all sharing one connection.

Handlers are ``REQUEST``-scoped for a reason that is easy to miss: a handler that
received a fresh session on each dependency would commit a different transaction
than the one its repositories wrote to.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from cqrs.requests.map import RequestMap, SagaMap
from cqrs.saga.storage.protocol import ISagaStorage
from dishka import Provider, Scope, provide
from faststream.kafka import KafkaBroker
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from plantkeeper.application.commands.care import SkipWateringHandler, WaterPlantHandler
from plantkeeper.application.commands.catalog import RequestSpeciesSyncHandler
from plantkeeper.application.commands.garden import (
    AddPlantHandler,
    CreateHouseholdHandler,
    MovePlantHandler,
    RemovePlantHandler,
)
from plantkeeper.application.commands.notifications import AcknowledgeNotificationHandler
from plantkeeper.application.commands.telemetry import AddSensorHandler, RemoveSensorHandler
from plantkeeper.application.ports.catalog import SpeciesCache, SpeciesCatalog, SpeciesSource
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.event_publisher import EventPublisher
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
from plantkeeper.application.queries.care import GetTodayCareQueryHandler
from plantkeeper.application.queries.catalog import GetSpeciesQueryHandler, ListSpeciesQueryHandler
from plantkeeper.application.queries.garden import (
    GetHouseholdQueryHandler,
    GetPlantQueryHandler,
    ListPlantsQueryHandler,
)
from plantkeeper.application.queries.notifications import ListPendingNotificationsHandler
from plantkeeper.application.queries.telemetry import ListSensorsQueryHandler
from plantkeeper.application.registry import build_request_map
from plantkeeper.application.sagas.adapters import (
    OutboxSpeciesCache,
    RepositorySpeciesCatalog,
    UnconfiguredSpeciesSource,
)
from plantkeeper.application.sagas.adaptive_watering import AdaptiveWateringSaga
from plantkeeper.application.sagas.missed_care import MissedCareSaga
from plantkeeper.application.sagas.onboard import (
    CreateCareScheduleStep,
    CreateOnboardingNotificationStep,
    OnboardPlantSaga,
    OnboardPlantTrigger,
    PublishPlantOnboardedStep,
    ResolveSpeciesStep,
)
from plantkeeper.application.sagas.registry import build_saga_map
from plantkeeper.application.sagas.species_sync import (
    ApplySpeciesUpdatesStep,
    FetchSpeciesStep,
    InvalidateSpeciesCacheStep,
    SpeciesSyncSaga,
    SpeciesSyncTrigger,
)
from plantkeeper.infrastructure.clock import SystemClock
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.messaging.broker import build_broker
from plantkeeper.infrastructure.messaging.publisher import KafkaEventPublisher
from plantkeeper.infrastructure.messaging.relay import OutboxRelay
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
from plantkeeper.infrastructure.persistence.repositories.journal import (
    SqlAlchemyJournalEntryRepository,
)
from plantkeeper.infrastructure.persistence.repositories.notifications import (
    SqlAlchemyNotificationRepository,
)
from plantkeeper.infrastructure.persistence.repositories.telemetry import (
    SqlAlchemySensorRepository,
)
from plantkeeper.infrastructure.persistence.saga_storage import SqlAlchemySagaStorage
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork

HANDLER_TYPES = (
    # Commands
    CreateHouseholdHandler,
    AddPlantHandler,
    RemovePlantHandler,
    MovePlantHandler,
    WaterPlantHandler,
    SkipWateringHandler,
    RequestSpeciesSyncHandler,
    AddSensorHandler,
    RemoveSensorHandler,
    AcknowledgeNotificationHandler,
    # Queries
    GetPlantQueryHandler,
    ListPlantsQueryHandler,
    GetHouseholdQueryHandler,
    GetTodayCareQueryHandler,
    ListSpeciesQueryHandler,
    GetSpeciesQueryHandler,
    ListSensorsQueryHandler,
    ListPendingNotificationsHandler,
)
"""Every request handler, in the order the registry binds them."""


class AppProvider(Provider):
    """Process-wide configuration and the objects that outlive a request."""

    @provide(scope=Scope.APP)
    def settings(self) -> Settings:
        """Read the environment once."""
        return Settings()

    @provide(scope=Scope.APP)
    def clock(self) -> Clock:
        """Use the wall clock (tests replace this provider)."""
        return SystemClock()

    @provide(scope=Scope.APP)
    def engine(self, settings: Settings) -> AsyncEngine:
        """Create the async engine.

        ``pool_pre_ping`` is on because Postgres closes idle connections: a
        pooled connection that died during a quiet period would otherwise fail
        the next request.
        """
        return create_async_engine(settings.postgres_dsn, pool_pre_ping=True)

    @provide(scope=Scope.APP)
    def session_factory(self, engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        """Create the session factory.

        ``expire_on_commit=False`` keeps an aggregate usable after the commit
        that persisted it, which is what lets a handler answer with the view it
        built before committing.
        """
        return async_sessionmaker(engine, expire_on_commit=False)

    @provide(scope=Scope.APP)
    def request_map(self) -> RequestMap:
        """Build the command/query registry once per process."""
        return build_request_map()


class DatabaseProvider(Provider):
    """One session and one unit of work per request."""

    @provide(scope=Scope.REQUEST)
    async def session(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> AsyncIterator[AsyncSession]:
        """Open the request's session and close it when the request ends."""
        async with session_factory() as session:
            yield session

    @provide(scope=Scope.REQUEST)
    def unit_of_work(self, session: AsyncSession) -> UnitOfWork:
        """Wrap the request's session in a unit of work."""
        return SqlAlchemyUnitOfWork(session)


class RepositoryProvider(Provider):
    """The repository ports, for the read side that never commits."""

    @provide(scope=Scope.REQUEST)
    def tracker(self) -> AggregateTracker:
        """Give read repositories a tracker of their own.

        Reads never add or save, so nothing is tracked through these; the
        tracker exists because a repository needs one, and sharing the unit of
        work's tracker would only invite a read to publish something.
        """
        return AggregateTracker()

    @provide(scope=Scope.REQUEST)
    def plants(self, session: AsyncSession, tracker: AggregateTracker) -> PlantRepository:
        """Expose the plants of the request's session."""
        return SqlAlchemyPlantRepository(session, tracker)

    @provide(scope=Scope.REQUEST)
    def households(self, session: AsyncSession, tracker: AggregateTracker) -> HouseholdRepository:
        """Expose the households of the request's session."""
        return SqlAlchemyHouseholdRepository(session, tracker)

    @provide(scope=Scope.REQUEST)
    def care_schedules(
        self, session: AsyncSession, tracker: AggregateTracker
    ) -> CareScheduleRepository:
        """Expose the care schedules of the request's session."""
        return SqlAlchemyCareScheduleRepository(session, tracker)

    @provide(scope=Scope.REQUEST)
    def species(self, session: AsyncSession, tracker: AggregateTracker) -> SpeciesRepository:
        """Expose the catalogue of the request's session."""
        return SqlAlchemySpeciesRepository(session, tracker)

    @provide(scope=Scope.REQUEST)
    def sensors(self, session: AsyncSession, tracker: AggregateTracker) -> SensorRepository:
        """Expose the sensors of the request's session."""
        return SqlAlchemySensorRepository(session, tracker)

    @provide(scope=Scope.REQUEST)
    def journal_entries(
        self, session: AsyncSession, tracker: AggregateTracker
    ) -> JournalEntryRepository:
        """Expose the journal of the request's session."""
        return SqlAlchemyJournalEntryRepository(session, tracker)

    @provide(scope=Scope.REQUEST)
    def notifications(
        self, session: AsyncSession, tracker: AggregateTracker
    ) -> NotificationRepository:
        """Expose the notifications of the request's session."""
        return SqlAlchemyNotificationRepository(session, tracker)


class MessagingProvider(Provider):
    """Kafka, and the relay that drains the outbox into it."""

    @provide(scope=Scope.APP)
    def broker(self, settings: Settings) -> KafkaBroker:
        """Build the broker; the caller starts and stops it."""
        return build_broker(settings)

    @provide(scope=Scope.APP)
    def publisher(self, broker: KafkaBroker) -> EventPublisher:
        """Expose the Kafka event publisher through its port."""
        return KafkaEventPublisher(broker)

    @provide(scope=Scope.APP)
    def relay(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        settings: Settings,
    ) -> OutboxRelay:
        """Build the outbox relay, which owns its own sessions."""
        return OutboxRelay(session_factory=session_factory, publisher=publisher, settings=settings)


def build_handler_provider() -> Provider:
    """Return a provider that registers every handler at ``REQUEST`` scope.

    Built imperatively rather than declared as class attributes: the handler list
    then exists once, and ``HANDLER_TYPES`` can be asserted against the request
    map by a test.
    """
    provider = Provider(scope=Scope.REQUEST)
    for handler in HANDLER_TYPES:
        provider.provide(handler, scope=Scope.REQUEST)
    return provider


SAGA_COMPONENT_TYPES = (
    # Orchestration
    OnboardPlantSaga,
    ResolveSpeciesStep,
    CreateCareScheduleStep,
    CreateOnboardingNotificationStep,
    PublishPlantOnboardedStep,
    SpeciesSyncSaga,
    FetchSpeciesStep,
    ApplySpeciesUpdatesStep,
    InvalidateSpeciesCacheStep,
    # Choreography
    AdaptiveWateringSaga,
    MissedCareSaga,
    # Triggers
    OnboardPlantTrigger,
    SpeciesSyncTrigger,
)
"""Every saga, step handler and consumer, in the order the registry lists them."""


class SagaProvider(Provider):
    """The write-side consumers' dependencies.

    The division follows the lifetimes: the saga storage, the saga map and the
    upstream catalogue source live for the process; the sagas, their steps and the
    consumers (and the adapters that read through the request's session) live for
    one delivery.
    """

    @provide(scope=Scope.APP)
    def saga_storage(self, session_factory: async_sessionmaker[AsyncSession]) -> ISagaStorage:
        """Own the ``write_shared`` saga tables through the app's session factory."""
        return SqlAlchemySagaStorage(session_factory)

    @provide(scope=Scope.APP)
    def saga_map(self) -> SagaMap:
        """Bind each saga context type to its saga, once per process."""
        return build_saga_map()

    @provide(scope=Scope.APP)
    def species_source(self) -> SpeciesSource:
        """Answer the synchronisation saga; the Trefle adapter arrives in Phase 9."""
        return UnconfiguredSpeciesSource()

    @provide(scope=Scope.REQUEST)
    def species_catalog(self, species: SpeciesRepository) -> SpeciesCatalog:
        """Read the local catalogue through the onboarding saga's ACL."""
        return RepositorySpeciesCatalog(species)

    @provide(scope=Scope.REQUEST)
    def species_cache(self, unit_of_work: UnitOfWork) -> SpeciesCache:
        """Announce cache staleness through the request's outbox."""
        return OutboxSpeciesCache(unit_of_work)


def build_saga_component_provider() -> Provider:
    """Register every saga, step handler and consumer at ``REQUEST`` scope.

    Built imperatively for the same reason as the handlers: one list, which a test
    can compare with the registry so a component added to the code but not to the
    worker's container cannot go unnoticed.
    """
    provider = Provider(scope=Scope.REQUEST)
    for component in SAGA_COMPONENT_TYPES:
        provider.provide(component, scope=Scope.REQUEST)
    return provider


def worker_providers() -> list[Provider]:
    """Return the providers the outbox relay worker needs.

    The saga providers are here and not in ``api_providers``: the API writes to the
    outbox and never consumes, so building a saga storage in it would only add a
    second writer to the process that must not have one.
    """
    return [
        AppProvider(),
        DatabaseProvider(),
        RepositoryProvider(),
        MessagingProvider(),
        SagaProvider(),
        build_handler_provider(),
        build_saga_component_provider(),
    ]


def api_providers() -> list[Provider]:
    """Return the providers the HTTP API needs.

    The API never publishes: it writes to the outbox and the relay does the rest,
    so the Kafka broker is not part of its container.
    """
    return [
        AppProvider(),
        DatabaseProvider(),
        RepositoryProvider(),
        build_handler_provider(),
    ]
