# Changelog

All notable changes to this project are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
-

### Changed
-

### Deprecated
-

### Removed
-

### Fixed
-

### Security
-

## [0.1.0] — 2026-09-21

The first release: every roadmap phase is implemented. The Phase 10 entries come
first in each section because they describe the final shape of the repository; the
rest is the phase-by-phase history.

### Added
- Phase 10 contracts, generated from the code and never hand-written:
  `plantkeeper.api.openapi` exports `docs/openapi.json` — the same document FastAPI
  serves at `/docs`, built without a running process — and
  `plantkeeper.workers.asyncapi` / `plantkeeper.admin.asyncapi` export the two
  processes' Kafka subscriptions as AsyncAPI 3.0. `make contracts` runs all three;
  `uv run python tools/contracts.py --check` fails when a checked-in diagram is stale.
- The `x-plantkeeper-event-catalogue` extension on both AsyncAPI documents: every one
  of the 26 events with its topic, producer and consumers, derived from `EVENT_TOPICS`
  and the consumer registries. It exists because the write side's producers are the
  outbox relay, not FastStream routes, so a generated AsyncAPI document on its own
  cannot name them.
- `plantkeeper.infrastructure.contracts` — the derivations every document and diagram
  is built from (`catalogue.py`: the catalogue rows, the event-flow edge set and the
  saga sequences; `diagrams.py`: the two checked-in files' text).
- `tools/contracts.py` and the `make contracts` / `make diagrams` targets. The two
  AsyncAPI documents are generated in child processes each, because the read side
  needs Django configured and the write side must not import it.
- `docs/diagrams/event-flow.md` — the producer/consumer topology of the event
  catalogue, drawn from `EVENT_TOPICS` and the consumer registries — and
  `docs/diagrams/sagas.md` — the two orchestration sagas' step sequences, rendered by
  `python-cqrs`' own `SagaMermaid` from the real step lists. Both are committed so
  GitHub renders them, and both are guarded by `tests/unit/docs`.
- AsyncAPI channel titles for every subscription (`<topic> to <consumer group>`, plus
  a description naming the consumer). FastStream derives a channel key from the
  handler's function name when it has no title, and every handler here is called
  `handle` — without the titles, several groups on one topic collapse into a single
  channel and the document silently loses all but the last.
- `docs/patterns.md` — every pattern the platform uses, from bounded contexts to
  contract generation, each linked to the code that carries it, plus the deliberate
  non-patterns and the generated-artefact table.
- ADRs `0007-why-python-cqrs.md` (what is taken from the library, what is replaced,
  and why), `0008-outbox-pattern.md` (the outbox end to end: the producing half,
  the two consumer ledgers, the derived identifiers and the offset policy) and
  `0009-event-sourcing-journal.md` (why only the Journal is event-sourced, the unique
  `(stream_id, version)` as the optimistic lock, what a snapshot is for, and the
  corruption policy).
- `make coverage` — the full suite, an HTML report in `docs/coverage.html`, and the
  per-layer breakdown against its floors (domain 90%, application 80%, infrastructure
  70%). `README.md` publishes the current numbers: 100% / 97% / 96%.
- `tools/contracts.py --check` compares the generated documents and the checked-in
  diagrams with the code, so drift is a failing command rather than a review comment.
- The contract-test suites `tests/unit/contracts/` (OpenAPI, both AsyncAPI documents,
  including a warning-as-error check for the channel-collision failure) and
  `tests/unit/docs/` (the two diagrams).
- Phase 9 Trefle ACL: `plantkeeper.infrastructure.external.trefle` holds the wire models (`TrefleListResponse`, `TrefleSpeciesDetail`, `TrefleGrowth`), the mapping into domain values, and `TrefleClient` — the paginating client that reads `/species` and then one detail per species, wraps every request in an `aiolimiter` limiter (`TREFLE_REQUESTS_PER_MINUTE`, default 55), retries transient failures with `tenacity`, and translates HTTP failures into `TrefleAuthError`/`TrefleRecordMissingError`/`TrefleUnavailableError`.
- Phase 0 skeleton: uv workspace, monorepo layout, docker-compose (Kafka KRaft, Postgres, Valkey).
- Base `pyproject.toml` for all packages and apps.
- Shared configs: Ruff, Mypy strict, pytest-asyncio, pre-commit.
- `README.md`, `CONTRIBUTING.md`, `LICENSE.md`, `CHANGELOG.md`.
- `Makefile` with `dev`, `lint`, `test`, `migrate`, `iot`, `clean`.
- Phase 1 domain layer: typed value objects, entity/event base classes and seven bounded contexts (Garden, Care, Catalog, Journal, Telemetry, Notifications, Identity) with their invariants and 21 domain events.
- `docs/domain.md` ubiquitous language and aggregate map, `docs/events.md` event catalogue, and ADR `0002-bounded-contexts.md`.
- `make test-domain` — domain unit tests with the Phase 1 90% coverage floor.
- Phase 2 write side: application ports (`Repository`, `UnitOfWork`, `OutboxRepository`, `IdempotencyRepository`, `EventPublisher`, `Clock`), 18 command and query handlers, and a Pydantic view layer.
- SQLAlchemy 2.0 async persistence: ORM models, mappers and repositories for six write schemas, plus the shared `outbox` and `idempotency_keys` tables.
- Alembic migrations for the whole write schema (`make migrate`).
- Transactional outbox: `SqlAlchemyUnitOfWork.commit()` writes aggregates and outbox rows in one transaction, and `OutboxRelay` publishes them to Kafka with exponential backoff and a dead-letter topic after five failed polls.
- FastStream Kafka publisher, one topic per bounded context, with `event_name`/`event_id` headers and an aggregate-derived partition key.
- Dishka providers for settings, database, repositories, messaging and the CQRS mediator, plus `make api` and `make workers` entrypoints.
- FastAPI write API: plants, households, care, sensors, catalog and notifications, with `Idempotency-Key` support on create endpoints and one uniform error body (`{"detail", "error"}`: 409 for a broken invariant or a reused key, 404 for a missing aggregate, 422 for a body that cannot become a value object).
- `docs/adr/0003-write-side-outbox.md` and a transport section in `docs/events.md` (topics, body, headers, keys, delivery guarantees).
- End-to-end test of the write side: `POST /api/v1/plants` → 201, the row in Postgres, the event in the outbox, the relay, and `PlantAdded` readable from Kafka with its contract.
- Phase 3 read side: `apps/admin` is a Django application served by Starlette (`create_admin_application`) with a FastStream lifespan, its own `manage.py`, and a Django Admin whose local superuser is selected automatically (no login form).
- `read_analytics` schema, owned by Django migration `read_models.0001_read_models`: `plants`, `care_schedules`, `species`, `notifications`, `journal_entries` and the `processed_events` idempotency ledger.
- Five projections consuming Kafka inside the admin process — Garden, Care, Species, Notification and Journal — each with its own consumer group and idempotent on `(consumer_group, event_id)` in the same transaction as its writes.
- `EVENT_TYPES` (and `event_type_for`) in `plantkeeper.infrastructure.messaging.topics`: the `event_name` → event class registry a consumer deserialises with.
- Django Admin for every read model: filters, search, date drill-down, the journal inline on the plant page, read-only admins, and a browsable `processed_events` ledger.
- `make admin` (read side on :8001) and `make admin-static`; `make migrate` now applies the Alembic write schema **and** the Django read schema.
- `docs/cqrs.md`, `docs/adr/0004-read-side-projections.md`, and a read-side section in `docs/events.md`.
- Tests: the projection contract and behaviour (`tests/integration/test_projections.py`, including the transaction rollback of a failed projection), the read-model settings (`tests/unit/admin/`), and the end-to-end write → Kafka → projection → Django Admin path (`tests/e2e/test_cqrs_read_side.py`).
- Phase 4 sagas: `OnboardPlantSaga` (resolve the species, create the schedule, create the first reminder, publish `PlantOnboarded`) and `SpeciesSyncSaga` (fetch, diff, publish `SpeciesUpdated`, invalidate the cache) as `python-cqrs` orchestration sagas with per-step compensation; `AdaptiveWateringSaga` (dry soil pulls a far-away watering forward, overwatering raises one reminder) and `MissedCareSaga` (a 24-hour grace window ending in `CareMissed`) as choreography consumers.
- Four saga lifecycle events (`SagaStarted`, `SagaCompleted`, `SagaFailed`, `SagaCompensated`) on a new `saga.events` topic, taking the catalogue to 25.
- Write-side consumer infrastructure: `write_shared.processed_events` (idempotency on `(consumer_group, event_id)` in the same transaction as the work), `write_shared.saga_state` + `write_shared.saga_log` (Alembic `0002`, behind a project-owned `ISagaStorage`), and `write_care.missed_care_windows` (the grace-period timer).
- `apps/workers` now consumes: `register_consumers` attaches one group per saga to its topics before the broker starts, and the worker runs three background jobs next to the outbox relay — the missed-care tick, the daily catalogue trigger, and saga recovery.
- `docs/sagas.md`, `docs/adr/0005-orchestration-vs-choreography.md`, a saga section in `docs/events.md`, and `.env.example` entries for the worker settings.
- Tests: the saga registry and stop contracts (unit), the four sagas including both compensations and the grace period (integration), the schedulers and the recovery job against the real container (integration), and adding a plant → schedule + reminder + `PlantOnboarded` end to end (`tests/e2e/test_saga_flow.py`).
- Phase 5 IoT simulator: `tools/iot-simulator` gains a physical model (diurnal light, exponential soil drying, Gaussian noise with rare outliers), the five scenarios (`normal`, `drought`, `overwatering`, `cold_snap`, `sensor_failure`), an `aiokafka` publisher that batches into `telemetry.raw`, and a CLI (`--sensors`, `--interval`, `--scenario`, `--seed`, `--sensor-base-id`, `--replay`, `--dry-run`, `--max-ticks`). `make iot` and `make iot-drought` run it, and `make iot-dry-run` prints the stream without a broker.
- Telemetry ingress: `plantkeeper.application.telemetry.TelemetryIngestConsumer` validates a raw measurement, resolves `sensor_id → plant_id` through the sensor registry, stores the reading and appends the `TelemetryReceived` it produces to the transactional outbox in one transaction — only when the insert was the one that stored the row, so a redelivery stores and announces nothing new. `register_telemetry_ingest` subscribes it to `telemetry.raw` in `apps/workers`.
- `write_telemetry.sensor_readings` (Alembic `0003`): RANGE-partitioned by month on `recorded_at`, primary key `(sensor_id, recorded_at)` — at once the partition-key requirement and the ingress's idempotency key — with `plant_id` denormalised and no foreign key to `sensors`.
- `TelemetryRepository` port (`add_many` with `ON CONFLICT DO NOTHING`, `list_by_plant`), the `UnitOfWork.telemetry` property, and `plantkeeper.domain.telemetry.TelemetryReading` (the persisted fact: a reading plus the plant it belongs to).
- `TelemetryPartitionJob` and `plantkeeper.infrastructure.persistence.partitions`: a catch-all partition created by the migration, a monthly window kept three months ahead by the worker, and a failed tick that is logged and retried rather than fatal.
- `POST /api/v1/sensors` accepts an optional `sensor_id`, so a caller that already owns an identifier (the simulator prints its sensors' ids) can register exactly the sensor whose telemetry it will publish.
- `docs/iot-simulator.md`, `docs/telemetry.md`, the telemetry sections of `docs/events.md` and `docs/architecture.md`, and `.env.example` entries for `TELEMETRY_*`.
- Tests: the model, the scenarios, the publishers and the CLI loop (unit, `tests/unit/iot/`), the ingress's four decisions (unit, `tests/unit/application/test_telemetry_ingest.py`), the partition arithmetic and the worker's registration (unit), the readings table, its key, its partitions and its transactions against Postgres (integration), and the whole simulator → Kafka → table → outbox → saga path end to end (`tests/e2e/test_iot_flow.py`).
- Phase 6 event sourcing: `JournalAggregate` replays one plant's stream into state (`add_entry`, `apply`, `state`, `state_at`, the snapshot policy), and `JournalStream`/`JournalEntry` gain a lookup by identifier.
- `write_journal.event_store` (Alembic `0004`): one row per fact, keyed by `(stream_id, version)` — at once the optimistic lock and the replay order — with a unique `event_id`, a `global_position` identity column for `load_all`, and the event document in `jsonb` exactly as the outbox stores it. `write_journal.journal_snapshots` (Alembic `0005`) checkpoints a stream's state every 50 events.
- `EventStoreRepository` (`append`, `load_stream`, `load_all`) and `JournalSnapshotRepository` (`latest`, `save`) ports, the `UnitOfWork.event_store`/`journal_snapshots` properties, the request-scoped Dishka providers, and the SQLAlchemy implementations. A lost race for a version raises `EventStoreConcurrencyError` (a `ConcurrentWriteError`, now mapped to 409 by the API), and a stream that cannot be replayed raises `EventStoreCorruptionError` rather than being skipped.
- `AddJournalEntryCommand` + handler and the `GetJournalTimeline`/`GetJournalAtDate` queries, all reading the event store: the timeline replays the stream, and the dated answer keeps the entries whose care moment is at or before the end of that UTC day.
- `JournalEntryConsumer` turns a completed watering into a `WATERING` entry, registered with the worker's other consumers (`<prefix>-journal-entries`). It is idempotent twice over: the `(consumer_group, event_id)` ledger plus an entry id derived from the event, so even a rebuilt consumer group appends nothing.
- `GET /api/v1/journal/{plant_id}` and `GET /api/v1/journal/{plant_id}/at?date=…`.
- `docs/event-sourcing.md`, plus the journal sections of `docs/domain.md`, `docs/events.md` and `docs/cqrs.md`.
- Tests: the aggregate's replay, temporal state and snapshot policy (unit), the append and replay paths against an in-memory store (unit), the event/state mappers (unit), the store, its optimistic lock, `load_all` and the automatic checkpoint against Postgres (integration), the consumer's redelivery and rebuilt-group cases (integration), and watering over HTTP → Kafka → journal → dated replay (`tests/e2e/test_journal_flow.py`).
- Phase 7 gRPC: `proto/plantkeeper/v1/{common,care,garden}.proto` define `plantkeeper.v1.CareService` (`GetTodayCare`, `CompleteWatering`) and `plantkeeper.v1.GardenService` (`ListPlants`, `GetPlant`) over typed identifier messages, `google.protobuf.Timestamp`/`Duration`, and explicit presence for the optional fields, plus the shared `SensorReading` shape.
- `tools/protogen.py` and `make proto` generate the Python stubs into the gitignored `apps/api/src/plantkeeper/api/grpc/generated/` with `grpc_tools.protoc`, rewriting protoc's absolute proto-package imports to the generated package. The Makefile makes `proto` a prerequisite of `lint`, `test`, `test-unit`, `test-e2e` and `grpc`, so a fresh checkout needs no manual step.
- `plantkeeper.api.mediator` is the bridge both wire protocols use: `build_mediator` and `view_of` moved out of the FastAPI-only `deps`, leaving it the HTTP dependency alone.
- The gRPC servicers (`CareServicer`, `GardenServicer`) are thin adapters: one Dishka request scope per RPC, dispatch through the same mediator, and an explicit failure table (`plantkeeper.api.grpc.errors`) — `NotFoundError`→`NOT_FOUND`, `IdempotencyKeyConflictError`→`ALREADY_EXISTS`, `ConcurrentWriteError`→`ABORTED`, `DomainError`→`FAILED_PRECONDITION`, a malformed value→`INVALID_ARGUMENT`, anything else→`INTERNAL`.
- `make grpc` runs the server on `0.0.0.0:50051` (`python -m plantkeeper.api.grpc`): the two services, the health service and server reflection, reusing `api_providers()` and never building a Kafka broker.
- `docs/grpc.md` and `docs/adr/0006-rest-and-grpc.md`; `apps/api` gains runtime dependencies `protobuf`, `grpcio-health-checking` and `grpcio-reflection`, with `grpcio-tools` as a dev dependency.
- Tests: the error table and the view→wire mapping (unit), the generated stub contract (unit), and the served surface end to end — health, reflection, care, garden and the failure statuses (`tests/e2e/test_grpc_services.py`).
- Phase 8 notifications: `NotificationChannel` port (`publish`/`subscribe`, `NotificationSubscription.wait`, `NotificationChannelError`) and the Valkey adapter `ValkeyNotificationChannel` over a per-household channel `household:<id>`, provided by a new `NotificationProvider`.
- `NotificationConsumer` turns `WateringDue`, `WateringRescheduled`, `CareMissed`, `SoilMoistureLow` and `TemperatureAnomaly` into notifications, and `NotificationPusher` consumes `NotificationCreated` and wakes the household's long poll. Both are ordinary write-side consumers with their own groups and `(consumer_group, event_id)` ledgers, registered in the saga registry and `make workers`.
- `GET /api/v1/notifications/pending?household_id=…&timeout=…` is now an HTTP long poll: it answers `200` with the pending notifications, waits on the household's channel when there are none, and answers `204 No Content` when the timeout passes with nothing. `timeout` defaults to `0` (immediate) and is capped at 60 seconds.
- The telemetry ingress now calls `Sensor.record(...)` and appends every event the reading raises to the outbox, so `SoilMoistureLow`, `SoilMoistureHigh` and `TemperatureAnomaly` have a real producer. `SoilMoistureHigh` now reaches `AdaptiveWateringSaga` in production, and `NotificationConsumer` consumes the low-moisture and temperature-anomaly ones.
- `NotificationView`/`NotificationResponse` gained `payload`, so a client can tell which plant a reminder concerns.
- Tests: the channel's publish/subscribe/timeout/isolation contract against a real Valkey (integration), the consumer and pusher including redelivery, rebuilt-group, unknown-plant and unread-guard cases (integration), the threshold events in the ingress (unit), and event → notification → long poll end to end (`tests/e2e/test_long_polling.py`, plus Valkey container fixtures in `tests/conftest.py`).
- `docs/notifications.md`.
- Phase 9 Trefle ACL: `plantkeeper.infrastructure.external.trefle` holds the wire models (`TrefleListResponse`, `TrefleSpeciesDetail`, `TrefleGrowth`), the mapping into domain values, and `TrefleClient` — the paginating client that reads `/species` and then one detail per species, wraps every request in an `aiolimiter` limiter (`TREFLE_REQUESTS_PER_MINUTE`, default 55), retries transient failures with `tenacity`, and translates HTTP failures into `TrefleAuthError`/`TrefleRecordMissingError`/`TrefleUnavailableError`.
- The Ellenberg mapping is documented and pinned by tests: `growth.light` (1–9) becomes `LOW`/`MEDIUM`/`HIGH`, and `growth.soil_humidity` (1–12) becomes a watering cadence through fixed bands (3/5/7/10/14/21 days). A Trefle slug becomes a stable `SpeciesId` through `uuid5` with a fixed namespace.
- `AsyncCircuitBreaker` in `plantkeeper.infrastructure.external.circuit_breaker`: consecutive failures open it, an elapsed reset timeout allows one trial, a success closes it. It reads the `Clock` port, so its timeout is tested by advancing a clock.
- `TrefleSpeciesSource` adds the fallback: the last successful upstream fetch is cached in Valkey (`species:upstream:snapshot`) and applied when Trefle is unavailable or its breaker is open. A failed fetch never yields a partial snapshot; an auth failure is never papered over.
- The Valkey species cache: `SpeciesCache`/`SpeciesCacheInvalidator` ports, `ValkeySpeciesCache` (key `species:<uuid>`, JSON `SpeciesView`, `SPECIES_CACHE_TTL_SECONDS` default 24 h), and `GetSpeciesQueryHandler` now reads through it.
- `SpeciesCacheConsumer` drops cached entries on `SpeciesUpdated` and `SpeciesCacheInvalidated`. It is an ordinary worker consumer with its own group (`<prefix>-species-cache`) and `(consumer_group, event_id)` ledger; the synchronisation's step 3 stays outbox-only, so no Valkey call happens inside a database transaction.
- `SpeciesAdded`, the catalogue's 26th domain event: `Species.add(...)` records it for an entry the synchronisation creates, and `SpeciesProjection` consumes it with the same upsert as `SpeciesUpdated`. The synchronisation's compensation deletes the species it created.
- `ExternalProvider` (worker-only) binds the Trefle source, or `UnconfiguredSpeciesSource` when `TREFLE_TOKEN` is empty; `NotificationProvider` became `ValkeyProvider`, which owns the one Valkey client and the species cache built on it.
- `docs/catalog.md` — the Trefle contract, the mapping tables, the limiter/retry/breaker order, the snapshot fallback, the cache and its invalidation, and the phase's known limits.
- Tests: the breaker's state machine over a movable clock, the mapping and the wire models, the client's pagination/retry/auth/404/breaker behaviour over `httpx.MockTransport`, the source's fallback, and the cache over a Valkey double (unit); the cache against a real Valkey, the cache consumer's ledger, the synchronisation's creation and its compensation, and the `SpeciesAdded` projection (integration); and the manual `POST /catalog/sync` → Trefle → 30 species → cache invalidation path end to end (`tests/e2e/test_catalog_sync.py`).

### Changed
- `docs/architecture.md` is the finished runtime view: the bounded-context table, the full context map (with ACL and event-carried state transfer), and sequence diagrams for the write path, the read path, the telemetry path and the saga path. The Phase 3 placeholder that was glued to the next heading is gone.
- `docs/patterns.md` and the ADR table in `README.md` are the entry points into the decisions; `README.md` also carries the final architecture flowchart, the `make contracts` / `make coverage` commands and the per-layer coverage numbers.
- `docs/sagas.md` and `docs/events.md` lost their Phase 4 "known limitations" that Phases 5, 8 and 9 resolved (no telemetry producer, placeholder Trefle source and cache), and gained the ones that are still true (no upstream species removal, `SensorOffline` without a producer).
- `docs/event-sourcing.md`, `docs/events.md` and ADR 0002 no longer announce Phase 10 work; they link to the ADR, the generated documents and the generated diagrams instead.
- `apps/workers`' consumer subscriptions and `apps/admin`'s projections gain AsyncAPI channel titles and descriptions. The runtime behaviour is unchanged; the generated documents are the difference.
- `plantkeeper.api.main` owns `VERSION` (the served OpenAPI `info.version`), and `plantkeeper.api.openapi` re-exports it, so the served document and the exported artefact cannot disagree.
- `docs/events.md` no longer carries a "Deferred to later phases" section: what remains is deferred permanently or is a recorded limitation.
- Scalar value objects (`Location`, `Moisture`, `Temperature`, `LightLevel`, the care intervals) now serialise as the scalar they wrap, so event payloads are flat (`"location": "Shelf"`) instead of nested (`"location": {"value": "Shelf"}`). Validation still accepts both shapes. Identifiers already behaved this way.
- `GET /api/v1/notifications/pending` answers `204 No Content` where it used to answer `200 {"items": []}`; a long poll has one way of saying "nothing", and the empty collection is gone.
- The `care_missed` notification moved from `MissedCareSaga` to `NotificationConsumer`: the saga owns the schedule and the grace window, and the Notifications context owns what the household sees. One producer per notification type.
- The worker now needs Valkey (`VALKEY_URL`), because `NotificationPusher` wakes long polls through it. A channel failure is logged and does not fail the delivery.
- `make test` now runs the integration and end-to-end suites as well; the container fixtures are lazy, so `make test-unit` still needs no Docker.
- Planned ADRs in `ROADMAP.md` are renumbered by one from Phase 4 onwards, because `docs/adr/0003-write-side-outbox.md` takes slot 3.
- `Settings` gains `read_side_consumer_group_prefix`; the read side reads the same `.env` as the services.
- `docs/events.md` lists each event's real consumer (the projections) instead of a planned one.
- Mypy ignores `django.*` imports and relaxes `disallow_subclassing_any` for `plantkeeper.admin.*`, because Django ships no `py.typed`; Ruff's RUF012 is off for the Django models and migrations, whose declarative class attributes are mutable by design.
- `Settings` gains `worker_consumer_group_prefix` and the four scheduler/recovery timers; `make workers` now runs the sagas' consumers and timers, not just the relay.
- `apps/workers` declares `python-cqrs` directly, as `apps/api` already did: the worker resolves each saga and its steps through the library's dispatcher.
- The Kafka decoder (`decode_event`, its header/body readers) moved from `apps/admin` into `plantkeeper.infrastructure.messaging.decoding`, so the read side's projections and the write side's sagas read one contract (`apps/admin` and `apps/workers` may not import each other).
- The `partition_key_for` field list gains `saga_id`, so a saga's four lifecycle events stay in one partition.
- `make iot` and `make iot-drought` no longer print a placeholder: they run the simulator. `make iot-dry-run` is new.
- `docs/events.md` no longer calls `TelemetryReceived`'s consumer "planned": every catalogued event now has a producer and a consumer.
- `tests/integration/test_migrations.py` counts modelled tables through `pg_class` (excluding partitions) instead of `pg_tables`, so the partitioned `sensor_readings` parent is included and its monthly children are not mistaken for models.
- The `sagas` package no longer re-exports its registry: the registry imports every consumer, and the journal's consumer imports the package for its base class, so the re-export would close an import cycle. The registry is imported by its own module name.
- `journal_entries` moved out of the read side's "built before their producer exists" list: a completed watering now produces `JournalEntryAdded`.
- `docs/events.md` names `JournalEntryConsumer` as `WateringCompleted`'s write-side consumer and lists its consumer group.
- `SpeciesSyncSaga`'s step 2 now creates a species the local catalogue does not know (`Species.add`) instead of skipping it, and its compensation deletes what the run created. Upstream removal is still not handled: a species that disappears from Trefle stays local.
- `SpeciesCatalog` (the onboarding saga's ACL) stays deliberately local-only: whether a plant's species is known is answered from `write_catalog`, never by an external HTTP call inside the saga.
- `NotificationProvider` is now `ValkeyProvider`: it owns the process's Valkey client and exposes both the notification channel and the species cache.
- `Settings`/`.env.example` gain the `TREFLE_*` variables and `SPECIES_CACHE_TTL_SECONDS`/`SPECIES_SNAPSHOT_TTL_SECONDS`; `packages/infrastructure` gains `aiolimiter`.
- `docs/events.md`, `docs/sagas.md`, `docs/cqrs.md` and `docs/architecture.md` reflect the catalogue's real producer, its consumers and the 26-event catalogue.

### Deprecated
-

### Removed
-

### Fixed
- `tests/e2e/test_write_side.py` picks its Kafka message by `event_id` instead of assuming it is the first `PlantAdded` on `garden.events`, which the read-side end-to-end tests made false.
-

### Security
-

## [0.0.0] — 2026-01-01

### Added
- Initial commit. Repository bootstrap.
