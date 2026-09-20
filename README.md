# 🌿 PlantKeeper

Event-driven plant care platform for a household plant collection, with simulated IoT
telemetry. One "household", several members, no authentication.

The project demonstrates **DDD + CQRS + Event Sourcing** on Python: a slow domain
(watering once a week) fed by a fast telemetry stream (a sensor reading every 10 seconds),
glued together with **Transactional Outbox**, **Idempotent Consumers**, **Sagas** and
**Kafka events** between bounded contexts.

## Stack

| Layer | Technology |
|-------|------------|
| Write API (REST) | FastAPI + Pydantic v2 + Dishka |
| Write API (gRPC) | grpcio + grpcio-tools + protobuf |
| Read / Admin | ASGI Django + Django Admin |
| Event streaming | FastStream + Kafka (KRaft, no Zookeeper) |
| CQRS framework | `python-cqrs` |
| ORM (write) | SQLAlchemy 2.0 async + Alembic |
| ORM (read / admin) | Django ORM (`read_analytics` schema) |
| Cache | Valkey |
| IoT simulator | own Python service with a physical model |
| Tests | pytest + pytest-asyncio + testcontainers |
| Tooling | uv workspace, Ruff, Mypy (strict), pre-commit |

## Repository layout

```
packages/domain          # aggregates, value objects, domain events (pure, no I/O)
packages/application     # commands, queries, sagas, ports
packages/infrastructure  # persistence, messaging, external ACLs, cache, DI
apps/api                 # FastAPI REST + gRPC servicers
apps/admin               # ASGI Django read models
apps/workers             # FastStream consumers
tools/iot-simulator      # Kafka telemetry generator
proto/                   # gRPC definitions
tests/                   # unit, integration, e2e
docs/                    # architecture, domain, ADRs
```

## Quick start

Requirements: Python 3.14+, [uv](https://docs.astral.sh/uv/), Docker with Compose v2.

```bash
uv sync --all-packages    # install the workspace
make dev                  # start Kafka (KRaft), Postgres, Valkey, Redpanda Console
make migrate              # apply the schemas (Alembic write_*, Django read_analytics)
make api                  # FastAPI on http://localhost:8000 (OpenAPI at /docs)
make workers              # outbox relay + telemetry ingress + the sagas and their timers
make admin                # read side on http://localhost:8001/admin/ (projections + Django Admin)
make iot                  # the IoT simulator: 20 sensors into telemetry.raw
make lint                 # ruff + mypy + import-linter
make test                 # pytest
make clean                # stop infra and drop volumes
```

The system runs as three processes on purpose. `make api` answers HTTP and commits
each command together with the events it produced into the outbox; `make workers`
runs the relay that publishes those events to Kafka, the write-side consumers (the
telemetry ingress that turns `telemetry.raw` into readings and events, and the sagas),
and their timers (the missed-care tick, the daily catalogue trigger, saga recovery and
the readings table's partition window); `make admin` consumes them into the
`read_analytics` schema and serves Django Admin over it. Nothing in the API process
talks to the broker, so a slow Kafka cannot slow a request down, and nothing in the
read side reads a write schema. See [`docs/events.md`](docs/events.md) for the topic,
header and key contract, [`docs/telemetry.md`](docs/telemetry.md) for the raw stream
and the readings table, [`docs/cqrs.md`](docs/cqrs.md) for the two sides, and
[`docs/sagas.md`](docs/sagas.md) for the process managers.

`make iot` is only useful once a sensor has somewhere to report to: the simulator
prints the ids it generates, and readings from an unregistered sensor are dropped (with
a warning) because no plant owns them yet. Register one with
`POST /api/v1/sensors {"plant_id": "...", "sensor_id": "<the printed id>"}`, or read
[`docs/iot-simulator.md`](docs/iot-simulator.md) for the quicker path.

Local infrastructure endpoints:

| Service | Endpoint |
|---------|----------|
| Kafka | `localhost:9092` |
| Postgres | `localhost:5432` (database/user/password: `plantkeeper`) |
| Valkey | `localhost:6379` |
| Redpanda Console | <http://localhost:8080> |
| Django Admin (read side) | <http://localhost:8001/admin/> |

Connection settings are documented in `.env.example`.

## Documentation

- [`ROADMAP.md`](ROADMAP.md) — phases, atomic tasks, Definition of Done
- [`docs/architecture.md`](docs/architecture.md) — bounded contexts, layers, data flow
- [`docs/domain.md`](docs/domain.md) — ubiquitous language, aggregates, invariants
- [`docs/events.md`](docs/events.md) — event catalogue and its Kafka transport
- [`docs/telemetry.md`](docs/telemetry.md) — the raw `telemetry.raw` stream, the readings table and its partitions
- [`docs/iot-simulator.md`](docs/iot-simulator.md) — the simulator's physical model, scenarios and CLI
- [`docs/cqrs.md`](docs/cqrs.md) — write schema, read schema, projections
- [`docs/sagas.md`](docs/sagas.md) — the four process managers, their compensations and their timers
- [`docs/adr/`](docs/adr/) — architecture decision records
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — branches, commits, local workflow
- [`CHANGELOG.md`](CHANGELOG.md) — release history
- [`AGENTS.md`](AGENTS.md) — guidance for AI coding agents

The platform has no application authentication: the REST API takes no tokens and
tracks no sessions, and no bounded context models a login. Django Admin is a local
read-only view, so it renders as a single superuser that
`plantkeeper.admin.dev_auth` selects automatically — there is no login form to
reach. Set `DJANGO_AUTO_LOGIN_USER=` (empty) in `.env` to disable that and use
Django's own login instead, creating the user once with:

```bash
uv run python apps/admin/manage.py createsuperuser
```

Django Admin's own tables (`auth_*`, `django_*`) live in the `public` schema and
exist because the admin framework needs them; the platform's read models live in
`read_analytics` and are written only by projections.
