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
