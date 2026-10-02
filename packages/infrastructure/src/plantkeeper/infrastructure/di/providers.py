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

from collections.abc import AsyncIterator, Callable
from datetime import timedelta
from typing import cast

from aiolimiter import AsyncLimiter
from cqrs.requests.map import RequestMap, SagaMap
from cqrs.saga.storage.protocol import ISagaStorage
from dishka import Provider, Scope, provide
from faststream.kafka import KafkaBroker
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from valkey.asyncio import Valkey

from plantkeeper.application.catalog.consumer import SpeciesCacheConsumer
from plantkeeper.application.commands.care import (
    CreateCareScheduleHandler,
    DeleteCareScheduleHandler,
    SkipWateringHandler,
    WaterPlantHandler,
)
from plantkeeper.application.commands.catalog import RequestSpeciesSyncHandler
from plantkeeper.application.commands.garden import (
    AddPlantHandler,
    CreateHouseholdHandler,
    MovePlantHandler,
    RemovePlantHandler,
)
from plantkeeper.application.commands.journal import AddJournalEntryHandler
from plantkeeper.application.commands.notifications import (
    AcknowledgeNotificationHandler,
    CreateOnboardingNotificationHandler,
    DeleteNotificationHandler,
)
from plantkeeper.application.commands.telemetry import AddSensorHandler, RemoveSensorHandler
from plantkeeper.application.journal.consumer import JournalEntryConsumer
from plantkeeper.application.notifications.consumer import NotificationConsumer
from plantkeeper.application.notifications.pusher import NotificationPusher
from plantkeeper.application.notifications.stream import (
    HouseholdSignalFanout,
    NotificationStreamService,
)
from plantkeeper.application.ports.catalog import (
    SpeciesCache,
    SpeciesCacheInvalidator,
    SpeciesCatalog,
    SpeciesSource,
)
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.dead_letter import DeadLetterPublisher
from plantkeeper.application.ports.event_publisher import EventPublisher
from plantkeeper.application.ports.event_store import (
    EventStoreRepository,
    JournalSnapshotRepository,
)
from plantkeeper.application.ports.notifications import (
    NotificationChannel,
    PendingNotificationReader,
)
from plantkeeper.application.ports.plant_references import (
    JournalPlantRefs,
    NotificationPlantRefs,
)
from plantkeeper.application.ports.read_models import ReadModelReader
from plantkeeper.application.ports.repositories import (
    CareScheduleRepository,
    HouseholdRepository,
    JournalEntryRepository,
    NotificationRepository,
    PlantRepository,
    SensorRepository,
    SpeciesRepository,
    TelemetryRepository,
)
from plantkeeper.application.ports.saga_intents import SagaIntentRepository
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.queries.care import GetTodayCareQueryHandler
from plantkeeper.application.queries.catalog import GetSpeciesQueryHandler, ListSpeciesQueryHandler
from plantkeeper.application.queries.garden import (
    GetHouseholdQueryHandler,
    GetPlantQueryHandler,
    ListPlantsQueryHandler,
)
from plantkeeper.application.queries.journal import (
    GetJournalAtDateHandler,
    GetJournalTimelineHandler,
)
from plantkeeper.application.queries.notifications import ListPendingNotificationsHandler
from plantkeeper.application.queries.telemetry import ListSensorsQueryHandler
from plantkeeper.application.registry import build_command_map, build_request_map
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
from plantkeeper.application.telemetry.ingest import TelemetryIngestConsumer
from plantkeeper.infrastructure.cache.species import ValkeySpeciesCache
from plantkeeper.infrastructure.clock import SystemClock
from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.external.circuit_breaker import AsyncCircuitBreaker
from plantkeeper.infrastructure.external.trefle.client import TrefleClient
from plantkeeper.infrastructure.external.trefle.source import (
    TrefleSpeciesSource,
    ValkeySpeciesSnapshotStore,
)
from plantkeeper.infrastructure.messaging.broker import build_broker
from plantkeeper.infrastructure.messaging.publisher import KafkaEventPublisher
from plantkeeper.infrastructure.messaging.relay import OutboxRelay
from plantkeeper.infrastructure.notifications.channel import ValkeyNotificationChannel
from plantkeeper.infrastructure.notifications.reader import SqlAlchemyPendingNotificationReader
from plantkeeper.infrastructure.persistence.models.plant_refs import (
    JournalPlantReferenceModel,
    NotificationPlantReferenceModel,
)
from plantkeeper.infrastructure.persistence.repositories.care import (
    SqlAlchemyCareScheduleRepository,
)
from plantkeeper.infrastructure.persistence.repositories.catalog import (
    SqlAlchemySpeciesRepository,
)
from plantkeeper.infrastructure.persistence.repositories.event_store import (
    SqlAlchemyEventStoreRepository,
    SqlAlchemyJournalSnapshotRepository,
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
from plantkeeper.infrastructure.persistence.repositories.plant_references import (
    SqlAlchemyPlantReferenceRepository,
)
from plantkeeper.infrastructure.persistence.repositories.read_models import (
    SqlAlchemyReadModelReader,
)
from plantkeeper.infrastructure.persistence.repositories.sagas import (
    SqlAlchemySagaIntentRepository,
)
from plantkeeper.infrastructure.persistence.repositories.telemetry import (
    SqlAlchemySensorRepository,
    SqlAlchemyTelemetryRepository,
)
from plantkeeper.infrastructure.persistence.saga_storage import (
    SagaCommitter,
    SqlAlchemySagaStorage,
)
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork

HANDLER_TYPES = (
    # Commands
    CreateHouseholdHandler,
    CreateCareScheduleHandler,
    DeleteCareScheduleHandler,
    AddPlantHandler,
    RemovePlantHandler,
    MovePlantHandler,
    WaterPlantHandler,
    SkipWateringHandler,
    RequestSpeciesSyncHandler,
    AddSensorHandler,
    RemoveSensorHandler,
    AcknowledgeNotificationHandler,
    CreateOnboardingNotificationHandler,
    DeleteNotificationHandler,
    AddJournalEntryHandler,
    # Queries
    GetPlantQueryHandler,
    ListPlantsQueryHandler,
    GetHouseholdQueryHandler,
    GetTodayCareQueryHandler,
    ListSpeciesQueryHandler,
    GetSpeciesQueryHandler,
    ListSensorsQueryHandler,
    ListPendingNotificationsHandler,
    GetJournalTimelineHandler,
    GetJournalAtDateHandler,
)
"""Every request handler, in the order the registry binds them."""


class AppProvider(Provider):
    """Process-wide configuration and the objects that outlive a request.

    ``request_map`` is a callable rather than a fixed function because the two
    deployables register different halves of the registry — see
    :meth:`request_map`.
    """

    def __init__(self, *, request_map: Callable[[], RequestMap] | None = None) -> None:
        super().__init__()
        self._request_map = request_map if request_map is not None else build_request_map

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
        """Build this process's request registry once per process.

        The worker gets the *command* map and the API the full one: Dishka builds a
        handler's dependencies when the handler is resolved, and the dispatcher
        resolves every entry to find one by name, so a worker holding the query map
        would need the read-side engine for no reason.
        """
        return self._request_map()


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
    def telemetry(self, session: AsyncSession) -> TelemetryRepository:
        """Expose the readings of the request's session.

        No tracker: a reading is an append-only fact, not an aggregate that could
        record an event.
        """
        return SqlAlchemyTelemetryRepository(session)

    @provide(scope=Scope.REQUEST)
    def journal_entries(
        self, session: AsyncSession, tracker: AggregateTracker
    ) -> JournalEntryRepository:
        """Expose the journal of the request's session."""
        return SqlAlchemyJournalEntryRepository(session, tracker)

    @provide(scope=Scope.REQUEST)
    def event_store(self, session: AsyncSession) -> EventStoreRepository:
        """Expose the journal's event stream of the request's session.

        No tracker: an appended event is published explicitly by the code that
        appends it, because the store is not an aggregate repository.
        """
        return SqlAlchemyEventStoreRepository(session)

    @provide(scope=Scope.REQUEST)
    def journal_snapshots(self, session: AsyncSession) -> JournalSnapshotRepository:
        """Expose the journal's checkpoints of the request's session."""
        return SqlAlchemyJournalSnapshotRepository(session)

    @provide(scope=Scope.REQUEST)
    def notifications(
        self, session: AsyncSession, tracker: AggregateTracker
    ) -> NotificationRepository:
        """Expose the notifications of the request's session."""
        return SqlAlchemyNotificationRepository(session, tracker)

    @provide(scope=Scope.REQUEST)
    def notification_plant_refs(self, session: AsyncSession) -> NotificationPlantRefs:
        """The notifications context's own view of the Garden context's plants.

        The declared type is the context's own name for the port, not
        ``PlantReferenceRepository``: Dishka keys a factory by its return type
        alone, so two providers returning the shared port would collide and the
        last one registered would answer for both consumers. The symptoms are
        quiet — a reminder addressed from the journal's table — and the name is
        what makes the graph unable to mix the two.
        """
        return SqlAlchemyPlantReferenceRepository(session, NotificationPlantReferenceModel)

    @provide(scope=Scope.REQUEST)
    def journal_plant_refs(self, session: AsyncSession) -> JournalPlantRefs:
        """The journal's own view of the Garden context's plants, separately named."""
        return SqlAlchemyPlantReferenceRepository(session, JournalPlantReferenceModel)

    @provide(scope=Scope.REQUEST)
    def saga_intents(self, session: AsyncSession) -> SagaIntentRepository:
        """Expose the recorded cross-context commands of the request's session.

        Request-scoped although a saga step records into it and the dispatcher
        claims from it: both want the same transaction their other work is in, and
        a process-scoped repository would always be writing through a session
        nobody else could see.
        """
        return SqlAlchemySagaIntentRepository(session)


class ReadEngine:
    """The *read* instance's engine, as a dependency of its own.

    Both instances are :class:`~sqlalchemy.ext.asyncio.AsyncEngine`, so two
    providers returning *that* type collide: Dishka keys a factory by its return
    type alone — an ``Annotated`` alias does not separate them, whichever order the
    providers are registered in — and the last registration wins. The symptom is
    quiet and total: every command runs against the read-only database, where the
    write schema does not exist, and every query against the write one, where the
    read models do not.

    A wrapper rather than a subclass: SQLAlchemy engines are created by a factory,
    so a subclass would have to be constructed by copying one and would then be a
    second dialect instance pretending to be the first. This holds the real engine
    and exists only to give the graph a name for it.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @property
    def engine(self) -> AsyncEngine:
        """The read instance's engine, for the factories that need the real thing."""
        return self._engine


class ReadSessionFactory:
    """The *read* instance's session factory, as a dependency of its own.

    The same collision as :class:`ReadEngine`, one level down: this is an
    ``async_sessionmaker[AsyncSession]`` and so is the write side's, so without a
    name of its own the last registration wins and the unit of work opens the
    wrong database.
    """

    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self._factory = factory

    @property
    def factory(self) -> async_sessionmaker[AsyncSession]:
        """The read session factory, for the reader that needs the real thing."""
        return self._factory


class ReadModelProvider(Provider):
    """The read side's database, for the queries answered from read models.

    Separate from :class:`DatabaseProvider` on purpose. This is a *second*
    Postgres, and only the processes that answer a client query may hold it: a
    worker or the read-side admin process that built it would open a connection
    nobody uses. ``Scope.APP`` because the engine and its pool must outlive a
    request; the reader itself opens a short session per query, so a request that
    touches the read side holds no read connection while it does anything else.

    Nothing here is created eagerly either. Dishka builds a provider's object the
    first time a dependency asks for it, so an API process that only ever serves
    commands never opens the read instance at all — which is what the spec means
    by "the read side is unavailable, and only the query side is degraded".
    """

    @provide(scope=Scope.APP)
    async def read_engine(self, settings: Settings) -> AsyncIterator[ReadEngine]:
        """Open the read engine, read-only, and release its pool on shutdown.

        ``default_transaction_read_only`` is what makes the read-only promise a
        property of the connection rather than a convention: a write attempted
        through this engine is refused by Postgres. ``pool_pre_ping`` for the same
        reason as the write engine's — the read instance closes idle connections
        too.
        """
        engine = create_async_engine(
            settings.read_postgres_dsn,
            pool_pre_ping=True,
            connect_args={"server_settings": {"default_transaction_read_only": "on"}},
        )
        try:
            yield ReadEngine(engine)
        finally:
            await engine.dispose()

    @provide(scope=Scope.APP)
    def read_session_factory(self, read_engine: ReadEngine) -> ReadSessionFactory:
        """Create the read session factory, under a name of its own.

        Wrapped for the same reason the engine is the collision above is not the
        only one: this factory and the write one are both
        ``async_sessionmaker[AsyncSession]``, and Dishka would hand the write
        side's session to whoever asked for either. The unit of work would then
        open the read instance's sessions — and, since that instance has no write
        schema and refuses writes, every command would fail there.
        """
        return ReadSessionFactory(async_sessionmaker(read_engine.engine, expire_on_commit=False))

    @provide(scope=Scope.APP)
    def read_models(self, read_session_factory: ReadSessionFactory) -> ReadModelReader:
        """Expose the read models through the application's query port."""
        return SqlAlchemyReadModelReader(read_session_factory.factory)


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
    def dead_letter_publisher(self, broker: KafkaBroker) -> DeadLetterPublisher:
        """Expose the dead-letter half of the same wrapper through its own port.

        Its own factory rather than a second return type on the one above: Dishka
        keys a factory by its return annotation and the last registration wins, so
        one factory answering for two ports would hand every consumer whichever
        port was registered last. Two factories, one thin wrapper, no state.
        """
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


class ValkeyProvider(Provider):
    """The one Valkey client of the process, and what is built on top of it.

    Short-lived data lives here: the presence channel a long poll waits on, and
    the catalogue cache. The client lives for the process and is created lazily,
    so a container that never resolves either — the gRPC process, a test that only
    builds handlers — never opens a connection to Valkey. Subscriptions do not use
    this client's connection: the channel adapter opens one per subscription.
    """

    @provide(scope=Scope.APP)
    async def valkey(self, settings: Settings) -> AsyncIterator[Valkey]:
        """Connect on first use and release the connection pool on shutdown."""
        client = Valkey.from_url(settings.valkey_url, decode_responses=True)
        try:
            yield client
        finally:
            await client.aclose()

    @provide(scope=Scope.APP)
    def notification_channel(self, valkey: Valkey) -> NotificationChannel:
        """Expose the channel through its application-layer port."""
        return ValkeyNotificationChannel(valkey)

    @provide(scope=Scope.APP)
    def species_cache(self, valkey: Valkey, settings: Settings) -> SpeciesCache:
        """Cache catalogue reads in Valkey, with the configured TTL."""
        return ValkeySpeciesCache(valkey, ttl_seconds=settings.species_cache_ttl_seconds)


class NotificationStreamProvider(Provider):
    """The API's streaming delivery: one signal subscription per household.

    Process-scoped on purpose: the fan-out is what bounds a household to one
    subscription however many streams are open, and a request-scoped fan-out could
    not. The reader is the other half of the same requirement — it opens a session
    per read and holds none in between, so an idle stream owns no database
    connection at all.

    API-only: the worker nudges households, it never streams, so
    ``worker_providers`` does not build any of this.
    """

    @provide(scope=Scope.APP)
    async def household_fanout(
        self, channel: NotificationChannel
    ) -> AsyncIterator[HouseholdSignalFanout]:
        """Own the process's per-household subscriptions until shutdown."""
        fanout = HouseholdSignalFanout(channel)
        try:
            yield fanout
        finally:
            await fanout.aclose()

    @provide(scope=Scope.APP)
    def pending_notification_reader(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> PendingNotificationReader:
        """Read a stream's batches on sessions of their own."""
        return SqlAlchemyPendingNotificationReader(session_factory)

    @provide(scope=Scope.APP)
    def notification_stream(
        self,
        fanout: HouseholdSignalFanout,
        reader: PendingNotificationReader,
    ) -> NotificationStreamService:
        """Bind the stream service over the process's fan-out and reader."""
        return NotificationStreamService(fanout, reader)


class ExternalProvider(Provider):
    """The Trefle anti-corruption layer.

    Worker-only: the API never publishes, and the synchronisation runs in the
    worker, so a process that only serves HTTP has no reason to hold an outbound
    HTTP client. With no token configured the ``SpeciesSource`` binding is the
    no-op source, which keeps a local checkout working.
    """

    @provide(scope=Scope.APP)
    async def trefle_http_client(self, settings: Settings) -> AsyncIterator[AsyncClient]:
        """Open the Trefle client and close its connection pool on shutdown."""
        async with AsyncClient(
            base_url=settings.trefle_base_url,
            timeout=settings.trefle_request_timeout_seconds,
        ) as client:
            yield client

    @provide(scope=Scope.APP)
    def trefle_limiter(self, settings: Settings) -> AsyncLimiter:
        """Hold every Trefle request under the configured per-minute ceiling."""
        return AsyncLimiter(settings.trefle_requests_per_minute, 60)

    @provide(scope=Scope.APP)
    def trefle_breaker(self, settings: Settings, clock: Clock) -> AsyncCircuitBreaker:
        """One breaker for the whole process, so the failure count means something."""
        return AsyncCircuitBreaker(
            name="trefle",
            failure_threshold=settings.trefle_breaker_failure_threshold,
            reset_timeout=timedelta(seconds=settings.trefle_breaker_reset_seconds),
            clock=clock,
        )

    @provide(scope=Scope.APP)
    def species_source(
        self,
        settings: Settings,
        client: AsyncClient,
        limiter: AsyncLimiter,
        breaker: AsyncCircuitBreaker,
        valkey: Valkey,
    ) -> SpeciesSource:
        """Bind the Trefle source, or the no-op one when no token is configured."""
        if not settings.trefle_token:
            return UnconfiguredSpeciesSource()
        return TrefleSpeciesSource(
            client=TrefleClient(
                client=client,
                limiter=limiter,
                breaker=breaker,
                token=settings.trefle_token,
                species_limit=settings.trefle_species_limit,
                max_attempts=settings.trefle_max_attempts,
            ),
            snapshots=ValkeySpeciesSnapshotStore(
                valkey, ttl_seconds=settings.species_snapshot_ttl_seconds
            ),
        )


def build_handler_provider(request_map: RequestMap | None = None) -> Provider:
    """Return a provider that registers this process's handlers at ``REQUEST`` scope.

    Built imperatively rather than declared as class attributes: the handler list
    then exists once, and ``HANDLER_TYPES`` can be asserted against the request
    map by a test.

    ``request_map`` narrows the registration to the handlers a process actually
    dispatches. Dishka builds a handler's whole dependency graph when the container
    is validated, so registering the query handlers in a worker — which answers no
    queries and never opens the read-side database — would demand the read models'
    engine for nothing.
    """
    handlers: tuple[type[object], ...] = HANDLER_TYPES
    if request_map is not None:
        bound = set(request_map.values())
        handlers = tuple(handler for handler in HANDLER_TYPES if handler in bound)
    provider = Provider(scope=Scope.REQUEST)
    for handler in handlers:
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
    JournalEntryConsumer,
    NotificationConsumer,
    NotificationPusher,
    SpeciesCacheConsumer,
    # Ingress
    TelemetryIngestConsumer,
    # Triggers
    OnboardPlantTrigger,
    SpeciesSyncTrigger,
)
"""Every saga, step handler and consumer, in the order the registry lists them.

``TelemetryIngestConsumer`` is here even though it is not in the application's
saga registry: it reads a raw topic rather than a domain event, so the worker
subscribes it explicitly, but it still needs the request scope's session and
repositories to do its work.
"""


class SagaProvider(Provider):
    """The write-side consumers' dependencies.

    The division follows the lifetimes: the saga map lives for the process; the sagas,
    their steps, the consumers, the *request-bound* saga storage and the adapters that
    read through the request's session live for one delivery. The upstream catalogue
    source is not here — it belongs to :class:`ExternalProvider`.

    ``ISagaStorage`` is therefore bound **once**, at ``REQUEST`` scope. A process-scoped
    binding cannot sit beside it: a container holds one factory per type, so the second
    registration replaces the first instead of being chosen by scope. Registered last,
    a process-scoped one leaves the lookup a request makes without a factory at all;
    registered first, it makes the request scope answer with the *unbound* storage and
    a saga step's checkpoint stops sharing the delivery's transaction. The one caller
    with no request — the worker's entry point — builds the unbound storage itself
    (``plantkeeper.workers.main``).
    """

    @provide(scope=Scope.REQUEST)
    def request_saga_storage(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        session: AsyncSession,
        unit_of_work: UnitOfWork,
    ) -> ISagaStorage:
        """Bind the saga tables to the request's own transaction.

        The engine commits at every step checkpoint, and with this binding those
        commits are the request's: a step's effect, its step-history entry, its
        checkpoint and its lifecycle outbox row become durable together or not at
        all, which is the crash window this closes (``docs/sagas.md``).

        It is the only binding of ``ISagaStorage`` the container holds, so a consumer's
        saga gets the bound one without anything in a request having to ask for it.

        The committer is the unit of work rather than the raw session, so a
        checkpoint commit drains the aggregates' events into the outbox on its way,
        exactly as a command handler's commit does. Committing the session directly
        would leave the outbox rows of everything the step touched unappended.
        """
        return SqlAlchemySagaStorage(
            session_factory, session=session, committer=cast("SagaCommitter", unit_of_work)
        )

    @provide(scope=Scope.APP)
    def saga_map(self) -> SagaMap:
        """Bind each saga context type to its saga, once per process."""
        return build_saga_map()

    @provide(scope=Scope.REQUEST)
    def species_catalog(self, species: SpeciesRepository) -> SpeciesCatalog:
        """Read the local catalogue through the onboarding saga's ACL."""
        return RepositorySpeciesCatalog(species)

    @provide(scope=Scope.REQUEST)
    def species_cache_invalidator(self, unit_of_work: UnitOfWork) -> SpeciesCacheInvalidator:
        """Let the synchronisation saga declare staleness through its outbox.

        The actual ``DEL`` is ``SpeciesCacheConsumer``'s, once the event has
        travelled; the saga never opens a Valkey connection.
        """
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

    The saga providers, the notification channel and the Trefle ACL are here and
    not in ``api_providers``: the API writes to the outbox and never consumes, so
    building a saga storage in it would only add a second writer to the process
    that must not have one. The worker holds the channel because it is what wakes
    a household whose long poll is waiting, and the Trefle client because the
    catalogue synchronisation runs here.

    The read-model provider is deliberately absent: a worker consumes events and
    never answers a query, so it must not open a second database at all.
    """
    return [
        AppProvider(request_map=build_command_map),
        DatabaseProvider(),
        RepositoryProvider(),
        MessagingProvider(),
        ValkeyProvider(),
        SagaProvider(),
        ExternalProvider(),
        build_handler_provider(build_command_map()),
        build_saga_component_provider(),
    ]


def api_providers() -> list[Provider]:
    """Return the providers the HTTP API needs.

    The API never publishes: it writes to the outbox and the relay does the rest,
    so the Kafka broker is not part of its container. It does hold the
    notification channel, because the request-and-wait endpoint subscribes to it
    and the stream's fan-out keeps one subscription per household, the species
    cache, because ``GetSpeciesQuery`` reads through it, and the read-model
    provider, because the list and report queries are answered from the read
    instance; every one of those clients is only opened if a request that needs it
    arrives.
    """
    return [
        AppProvider(),
        DatabaseProvider(),
        RepositoryProvider(),
        ValkeyProvider(),
        NotificationStreamProvider(),
        ReadModelProvider(),
        build_handler_provider(build_request_map()),
    ]
