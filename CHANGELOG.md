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
- FastAPI write API: plants, households, care, sensors, catalog and notifications, with `Idempotency-Key` support on create endpoints and RFC 7807-style error handlers.
- `docs/adr/0003-write-side-outbox.md` and a transport section in `docs/events.md` (topics, body, headers, keys, delivery guarantees).
- End-to-end test of the write side: `POST /api/v1/plants` → 201, the row in Postgres, the event in the outbox, the relay, and `PlantAdded` readable from Kafka with its contract.

### Changed
- Scalar value objects (`Location`, `Moisture`, `Temperature`, `LightLevel`, the care intervals) now serialise as the scalar they wrap, so event payloads are flat (`"location": "Shelf"`) instead of nested (`"location": {"value": "Shelf"}`). Validation still accepts both shapes. Identifiers already behaved this way.
- `make test` now runs the integration and end-to-end suites as well; the container fixtures are lazy, so `make test-unit` still needs no Docker.
- Planned ADRs in `ROADMAP.md` are renumbered by one from Phase 4 onwards, because `docs/adr/0003-write-side-outbox.md` takes slot 3.

### Deprecated
-

### Removed
-

### Fixed
-

### Security
-

## [0.0.0] — 2026-01-01

### Added
- Initial commit. Repository bootstrap.
