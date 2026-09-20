# Changelog

All notable changes to this project are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
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

### Changed
- Scalar value objects (`Location`, `Moisture`, `Temperature`, `LightLevel`, the care intervals) now serialise as the scalar they wrap, so event payloads are flat (`"location": "Shelf"`) instead of nested (`"location": {"value": "Shelf"}`). Validation still accepts both shapes. Identifiers already behaved this way.
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
