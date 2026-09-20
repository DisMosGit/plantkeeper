# 0007. Why `python-cqrs`, and where the boundary with it is drawn

## Status

Accepted

## Date

2026-09-21

## Context

The project is a CQRS system with a mediator, a request registry, an outbox and four
sagas. Every one of those has a well-known implementation, so the first decision of
Phase 2 was not "how do we dispatch a command" but "which parts of that do we write
and which do we take from a library". `python-cqrs` was already in the stack (the
skeleton declared it for `packages/application`), so the question was concrete: for
each capability, does the library's version fit this project's contracts, or is it a
shape the project would have to work around?

Three of the project's contracts are non-negotiable and were fixed before the
comparison:

1. **The message contract** — an event's own JSON document as the Kafka body,
   `event_name`/`event_id` in the headers, an aggregate-derived key, and at-least-once
   delivery (`docs/events.md`).
2. **The write promise** — a `201` means the aggregate *and* the event's eventual
   publication are committed together (`docs/adr/0003-write-side-outbox.md`).
3. **The persistence shape** — one SQLAlchemy declarative base and one Alembic
   migration chain own every table, and the tables live in `write_*` schemas
   (`docs/adr/0003-write-side-outbox.md`).

## Decision

We will use `python-cqrs` for the **request side and the saga engine**, and own the
**outbox, the saga storage, the Kafka transport and the dependency injection**
ourselves.

**Taken as-is.**

- `PydanticRequest`/`PydanticResponse` for command and query DTOs, and `RequestMap`
  for the registry. The 26 use cases are ordinary Pydantic models, so validation is
  the same in the HTTP layer, the gRPC layer and the tests.
- `RequestMediator` (through `DishkaCQRSContainer`) as the dispatcher. Both wire
  protocols share one mediator instance per request, which is what makes the REST and
  gRPC surfaces wrappers over one application layer (`docs/adr/0006-rest-and-grpc.md`).
- `Saga`, `SagaStepHandler`, `SagaTransaction` and `SagaDispatcher` for the two
  orchestration sagas. The engine's compensation semantics — reverse order, refuse to
  move forward once a rollback started, resume a crashed saga from a step log — are
  the behaviour the project wants and did not want to re-derive
  (`docs/adr/0005-orchestration-vs-choreography.md`).
- The `ISagaStorage` **protocol**, which our `SqlAlchemySagaStorage` implements. The
  protocol is the useful half: the engine stays on its own abstraction, and the
  library's own implementation is not used for the reasons below.

**Replaced.**

- **The outbox.** `cqrs.outbox.sqlalchemy.OutboxModel` has no publication timestamp
  and no error text, no dead-letter path, and its Kafka adapter calls
  `producer.produce(topic, payload)` with no key and no headers — so adopting it
  would drop the message contract. The full comparison is in
  [`docs/adr/0003-write-side-outbox.md`](0003-write-side-outbox.md).
- **`SqlAlchemySagaStorage`.** Its tables are unqualified (`saga_executions`,
  `saga_logs`) and their names are read from the environment **at import time**, and
  it declares a second declarative base. Our adapter implements the same protocol
  over `write_shared.saga_state`/`saga_log`, which Alembic owns.
- **The Kafka message broker.** `cqrs.adapters.protocol.KafkaProducer` cannot express
  a key or a header, and the project's contract needs both.
- **Dependency injection.** The project uses Dishka directly. `python-cqrs` ships a
  container integration, but the composition root has to know about per-process
  providers (the worker builds the Trefle client and the relay; the API must not),
  which is a lifetime and scoping decision the library cannot make for us.

**Added, because the library stops short of it.** The four saga lifecycle events
(`SagaStarted`, `SagaCompleted`, `SagaFailed`, `SagaCompensated`) are ours: the
engine records a status, but nothing publishes it, and an operator who cannot see a
saga in the event stream can only inspect a table. The events travel to
`saga.events` through the outbox like every other fact.

## Consequences

- **Easier:** the request side is not bespoke. Commands, queries, their handlers and
  their registry are the library's own types, so there is no invented `Command` base
  to document or to teach.
- **Easier:** the saga engine's compensation is the library's, tested by it and by
  `tests/integration/test_sagas.py` against our storage.
- **Harder:** there is a boundary to keep in mind. "Is this library's version good
  enough?" is answered per capability, and the four replacements above are the
  answer for the four that are not. The boundary is written down here so the next
  person extending the platform does not relearn it by reading the library.
- **Harder:** `python-cqrs` ships no `py.typed` marker. A strict mypy build has an
  override for `cqrs.*` (`ignore_missing_imports`) and relaxes
  `disallow_subclassing_any`/`warn_return_any` for the modules that subclass its base
  classes, each with a comment saying why. The affected modules are listed in
  `[tool.mypy.overrides]` in the root `pyproject.toml`.
- **Constrained:** no library feature may publish to Kafka directly. Everything the
  write side produces goes through the outbox, which is the library's saga engine
  included — its step handlers write through our `UnitOfWork`.
- **Follow-up:** `SagaMermaid` (from the library) renders the checked-in
  [`docs/diagrams/sagas.md`](../diagrams/sagas.md). If the library is ever replaced,
  the generator goes with it; the diagram format is otherwise ours.

## References

- [`docs/adr/0003-write-side-outbox.md`](0003-write-side-outbox.md) — why our outbox,
  compared field by field with the library's
- [`docs/adr/0005-orchestration-vs-choreography.md`](0005-orchestration-vs-choreography.md)
  — the saga engine, the storage adapter and the library's import-time table names
- [`docs/adr/0006-rest-and-grpc.md`](0006-rest-and-grpc.md) — two protocols, one
  mediator
- [`pyproject.toml`](../../pyproject.toml) — the mypy overrides the untyped library
  requires
- [`packages/application/src/plantkeeper/application/sagas/`](../../packages/application/src/plantkeeper/application/sagas/)
  — the saga base class, the triggers and the registry
