# Patterns

> **Status:** every phase is implemented. Every pattern below is implemented, tested
> and referenced to the code that carries it. The catalogue is deliberately short: it
> lists the patterns this platform actually uses and where to read them, not a
> vocabulary of everything a distributed system could be built from.

The links point at the module that owns the pattern. A pattern with no single owner
is described in the document named in its row.

## Domain

| Pattern | Where | What it buys |
|---------|-------|--------------|
| **Bounded context** | [`packages/domain`](../packages/domain/src/plantkeeper/domain/), [ADR 0002](adr/0002-bounded-contexts.md) | Seven contexts (Garden, Care, Catalog, Journal, Telemetry, Notifications, Identity) share a kernel and nothing else. Enforced by the `independence` contract in [`pyproject.toml`](../pyproject.toml). |
| **Aggregate root** | [`domain/base.py`](../packages/domain/src/plantkeeper/domain/base.py) | One consistency boundary per invariant, and the only thing allowed to record an event. |
| **Value object** | [`domain/values.py`](../packages/domain/src/plantkeeper/domain/values.py), [`domain/identifiers.py`](../packages/domain/src/plantkeeper/domain/identifiers.py) | `Moisture`, `Temperature`, `WateringInterval`, `PlantId` … — validation at construction, no primitive obsession. |
| **Domain event** | [`domain/base.py`](../packages/domain/src/plantkeeper/domain/base.py) | A frozen Pydantic model with `event_id`/`occurred_at`; `_record()` on the aggregate that raised it (`docs/events.md`). |
| **Ubiquitous language** | [`docs/domain.md`](domain.md) | One glossary for the code, the events and the docs. |

## Application

| Pattern | Where | What it buys |
|---------|-------|--------------|
| **Command / query separation** | [`application/commands`](../packages/application/src/plantkeeper/application/commands/), [`application/queries`](../packages/application/src/plantkeeper/application/queries/) | A write returns an outcome, a read returns a view; both go through one `RequestMap` (`docs/cqrs.md`). |
| **Ports and adapters** | [`application/ports`](../packages/application/src/plantkeeper/application/ports/) | The use cases depend on `Protocol`s — `UnitOfWork`, `Clock`, `SpeciesCatalog`, `NotificationChannel` — never on SQLAlchemy, Kafka or Valkey. |
| **Orchestration saga** | [`application/sagas/base.py`](../packages/application/src/plantkeeper/application/sagas/base.py), [`onboard.py`](../packages/application/src/plantkeeper/application/sagas/onboard.py), [ADR 0005](adr/0005-orchestration-vs-choreography.md) | An ordered step list with per-step compensation, a deterministic `saga_id` and lifecycle events. |
| **Choreography** | [`application/sagas/adaptive_watering.py`](../packages/application/src/plantkeeper/application/sagas/adaptive_watering.py) | A reaction with no coordinator: one event in, one decision out. |
| **Recorded cross-context command** | [`application/ports/saga_intents.py`](../packages/application/src/plantkeeper/application/ports/saga_intents.py), [`scheduling/command_dispatch.py`](../packages/infrastructure/src/plantkeeper/infrastructure/scheduling/command_dispatch.py), [ADR 0012](adr/0012-saga-command-dispatch.md) | A step that changes a context the process does not own records a command in its own transaction; the dispatcher executes it through the owning context's handlers, at most once, so a process manager asks instead of writing another context's tables. |
| **Anti-corruption layer** | [`infrastructure/external/trefle`](../packages/infrastructure/src/plantkeeper/infrastructure/external/trefle/), [ADR 0005](adr/0005-orchestration-vs-choreography.md) | Trefle's wire format is translated at the boundary; the domain never sees it (`docs/catalog.md`). |

## Infrastructure

| Pattern | Where | What it buys |
|---------|-------|--------------|
| **Transactional outbox** | [`infrastructure/persistence/uow.py`](../packages/infrastructure/src/plantkeeper/infrastructure/persistence/uow.py), [`messaging/relay.py`](../packages/infrastructure/src/plantkeeper/infrastructure/messaging/relay.py), [ADR 0003](adr/0003-write-side-outbox.md) | The aggregate and the event it raised are committed in one transaction; the relay is the only producer. |
| **Outbox relay with DLQ** | [`messaging/relay.py`](../packages/infrastructure/src/plantkeeper/infrastructure/messaging/relay.py) | Per-message commit, `attempts` counted in failed polls, exponential backoff, and `plantkeeper.dlq.v1` after the threshold. |
| **Consumer failure policy** | [`application/delivery.py`](../packages/application/src/plantkeeper/application/delivery.py), [`messaging/failures.py`](../packages/infrastructure/src/plantkeeper/infrastructure/messaging/failures.py), [ADR 0011](adr/0011-consumer-failure-policy.md) | A domain-rule failure is moved aside at once, a retryable one is retried with backoff, and a delivery past its budget is copied to `plantkeeper.dlq.v1` with its ledger claim committed alongside the copy. |
| **Message provenance** | [`application/provenance.py`](../packages/application/src/plantkeeper/application/provenance.py), [`messaging/publisher.py`](../packages/infrastructure/src/plantkeeper/infrastructure/messaging/publisher.py), [ADR 0010](adr/0010-message-provenance.md) | `correlation_id`, `causation_id`, `raised_by`, `schema_version` and `traceparent` travel in headers and are copied onto every reaction, so a chain is walkable and an event's origin is never anonymous. |
| **Idempotent consumer** | [`messaging/decoding.py`](../packages/infrastructure/src/plantkeeper/infrastructure/messaging/decoding.py), [`application/sagas/consumer.py`](../packages/application/src/plantkeeper/application/sagas/consumer.py), [ADR 0008](adr/0008-outbox-pattern.md) | Every delivery is claimed in `write_shared.processed_events` (or the read schemas' `processed_events`) on `(consumer_group, event_id)`, in the same transaction as the work. |
| **Idempotency key** | [`application/idempotency.py`](../packages/application/src/plantkeeper/application/idempotency.py) | A create command replayed with the same `Idempotency-Key` returns the stored response; a different body under the same key is a conflict. |
| **Optimistic concurrency** | [`persistence/repositories/event_store.py`](../packages/infrastructure/src/plantkeeper/infrastructure/persistence/repositories/event_store.py) | The unique `(stream_id, version)` key is the lock; a lost race raises `EventStoreConcurrencyError`, mapped to HTTP 409. |
| **Event sourcing** | [`domain/journal`](../packages/domain/src/plantkeeper/domain/journal/), [`persistence/repositories/event_store.py`](../packages/infrastructure/src/plantkeeper/infrastructure/persistence/repositories/event_store.py), [ADR 0009](adr/0009-event-sourcing-journal.md) | The journal is an append-only stream replayed into state, with a checkpoint every 50 events (`docs/event-sourcing.md`). |
| **Saga recovery and retry budget** | [`scheduling/saga_recovery.py`](../packages/infrastructure/src/plantkeeper/infrastructure/scheduling/saga_recovery.py) | A saga left `running` by a crash is resumed from its step log, a recorded failure is retried within a bounded budget, and an exhausted budget parks the process for an operator to reset. |
| **Circuit breaker + retry + fallback** | [`external/circuit_breaker.py`](../packages/infrastructure/src/plantkeeper/infrastructure/external/circuit_breaker.py), [`external/trefle/source.py`](../packages/infrastructure/src/plantkeeper/infrastructure/external/trefle/source.py) | `tenacity` retries inside the breaker, the breaker stops a dead upstream, and the last successful snapshot is the fallback (`docs/catalog.md`). |
| **Cache-aside** | [`cache/species.py`](../packages/infrastructure/src/plantkeeper/infrastructure/cache/species.py) | `GetSpeciesQuery` reads through Valkey with a 24-hour TTL; invalidation is an event, not a call inside a transaction. |
| **Server-Sent Events, with request and wait as fallback** | [`api/rest/routers/notifications.py`](../apps/api/src/plantkeeper/api/rest/routers/notifications.py), [`application/notifications/stream.py`](../packages/application/src/plantkeeper/application/notifications/stream.py) | The stream pushes each notification as it appears, resumes from a cursor and holds no database session while idle; the request-and-wait form stays for clients that cannot stream (`docs/notifications.md`). |
| **Table partitioning and retention** | [`persistence/partitions.py`](../packages/infrastructure/src/plantkeeper/infrastructure/persistence/partitions.py) | `sensor_readings` is RANGE-partitioned by month, kept three months ahead by a worker job, with a catch-all default partition; whole months past the retention window are dropped while the rollups computed from them survive. |
| **Projection / read model** | [`admin/projections`](../apps/admin/src/plantkeeper/admin/projections/), [ADR 0004](adr/0004-read-side-projections.md) | One writer per read-model column, one consumer group per projection. |
| **Composition root** | [`infrastructure/di/providers.py`](../packages/infrastructure/src/plantkeeper/infrastructure/di/providers.py) | Dishka providers wire the ports to adapters per process; nothing else constructs an engine, a broker or a cache. |
| **Contract generation** | [`infrastructure/contracts`](../packages/infrastructure/src/plantkeeper/infrastructure/contracts/), [`tools/contracts.py`](../tools/contracts.py) | OpenAPI, AsyncAPI and the Mermaid diagrams are build products of the code (`make contracts`), never hand-written. |

## Contracts

| Document | Produced by | Committed? |
|----------|-------------|------------|
| `docs/openapi.json` | `python -m plantkeeper.api.openapi` | no (`.gitignore`) |
| `docs/asyncapi-read.json` | `python -m plantkeeper.admin.asyncapi` | no (`.gitignore`) |
| `docs/asyncapi-write.json` | `python -m plantkeeper.workers.asyncapi --catalogue-from docs/asyncapi-read.json` | no (`.gitignore`) |
| [`docs/diagrams/event-flow.md`](diagrams/event-flow.md) | `python tools/contracts.py --diagrams-only` | yes |
| [`docs/diagrams/sagas.md`](diagrams/sagas.md) | `python tools/contracts.py --diagrams-only` | yes |
| `docs/coverage.html` | `make coverage` | no (`.gitignore`) |
| `apps/api/src/plantkeeper/api/grpc/generated/` | `python tools/protogen.py` | no (`.gitignore`) |

`make contracts` writes every generated document, and
`uv run python tools/contracts.py --check` fails when a checked-in artefact no longer
matches the code. The diagrams are committed because GitHub renders a Mermaid block
in Markdown; the JSON documents are not, for the same reason the gRPC stubs are not
— a committed copy drifts from the contract it describes.

The order the generators run in is part of the contract. `plantkeeper.admin.asyncapi`
is the only generator that has Django configured, so it is the only one that can
describe *both* consumer sets: it writes the read-side document and the diagrams, and
`plantkeeper.workers.asyncapi` is handed that document's `x-plantkeeper-event-catalogue`
instead of building a write-side-only catalogue. Otherwise every event only a read
model consumes would be reported as consumed by nobody.

## Deliberate non-patterns

These are patterns a reader might expect and will not find, with the reason:

- **Synchronous HTTP between bounded contexts.** Forbidden by `AGENTS.md`; a context
  learns about another's facts from Kafka (`docs/events.md`). Trefle is the one
  external call, and it is wrapped in an ACL with a breaker.
- **A generic repository or a unit-of-work abstraction over Django.** The read side
  is a projection writer, not a domain; `AGENTS.md` keeps business logic out of the
  read side, and a repository there would be ceremony over one `update_or_create`.
- **A message bus in-process.** `python-cqrs`' mediator dispatches within a process;
  everything across a process boundary goes through the outbox and Kafka. Within the
  worker a saga changes another context by recording a command in
  `write_shared.saga_intents` for the dispatcher to execute — a table and a job, not a
  bus ([ADR 0012](adr/0012-saga-command-dispatch.md)).
- **Retry on a domain error.** `DomainError` and `ValidationError` are terminal —
  the delivery is answered, not retried; only infrastructure failures are retried
  (`docs/events.md`).
