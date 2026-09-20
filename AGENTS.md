# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project

**PlantKeeper** — event-driven plant care platform. DDD + CQRS + Event Sourcing for a household plant tracker with IoT telemetry simulation.

## Stack

- Python 3.14+, `uv` workspace (monorepo)
- FastAPI (REST) + gRPC + Django 6.1 (ASGI admin)
- FastStream + Kafka (KRaft)
- `python-cqrs` (commands, queries, events, sagas, outbox)
- SQLAlchemy 2.0 async + Alembic (write), Django ORM (read schema)
- Dishka (DI), Pydantic v2, Valkey, httpx
- Ruff + Mypy (strict) + pytest-asyncio + testcontainers

## Layout

- `packages/domain` — aggregates, value objects, events (no I/O)
- `packages/application` — commands, queries, sagas, ports
- `packages/infrastructure` — persistence, messaging, external, cache, DI
- `apps/api` — FastAPI REST + gRPC servicers
- `apps/admin` — ASGI Django read models
- `apps/workers` — FastStream consumers
- `tools/iot-simulator` — Kafka telemetry generator
- `proto/` — gRPC definitions
- `docs/` — architecture, domain, ADRs, runbooks

## Rules

- Domain layer is pure: no imports from infrastructure, FastAPI, Django, or SQLAlchemy.
- All cross-context communication goes through Kafka events, never direct imports.
- Every write use case goes through Unit of Work + Transactional Outbox.
- Every Kafka consumer must be idempotent on `(consumer_group, event_id)`.
- Schemas: Pydantic v2 for events, protobuf for gRPC. No dataclass payloads across boundaries.
- Type hints are mandatory. `mypy --strict` must pass.
- No `Any`, no `# type: ignore` without a comment explaining why.
- No auth in this project — do not add it.

## Commands

```
uv sync --all-packages      # install
uv run ruff check .         # lint
uv run ruff format .        # format
uv run mypy .               # typecheck
uv run pytest               # tests
docker compose up -d        # Kafka, Postgres, Valkey
uv run python -m plantkeeper.iot_simulator --scenario normal --seed 42
```

## When changing code

- Update `docs/events.md` if you add or rename a domain event.
- Add an ADR in `docs/adr/` for any architectural decision.
- Update AsyncAPI/OpenAPI artifacts when contracts change.
- Write a unit test for domain logic; an integration test for I/O.
- Keep commits conventional: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`.

## Do not

- Introduce Celery, RabbitMQ, or synchronous HTTP between bounded contexts.
- Bypass the Outbox for event publication.
- Put business logic in FastAPI routers or Django views.
- Add CI/CD, Kubernetes, or Terraform — out of scope.
