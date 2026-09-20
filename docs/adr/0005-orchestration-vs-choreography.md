# 0005. Orchestration vs choreography, and where saga state lives

## Status

Accepted

## Date

2026-09-21

## Context

Phase 4 adds four process managers (`ROADMAP.md` §4.1–4.6): onboarding a plant,
adapting a watering schedule to telemetry, escalating a missed watering after a
24-hour grace period, and synchronising the catalogue from an upstream source.
They are not alike. Two of them have a *sequence* — resolve the species, create a
schedule, create a reminder, announce the plant — where a failure halfway through
must undo what already happened. Two are *reactions*: when the soil is dry, move
the schedule; when the grace period expires, record the miss. There is no
coordinator in the second kind because there is nothing to coordinate: the
reaction is a function of one event and the current state.

`python-cqrs` 4.13 ships a saga engine: a `Saga` is an ordered list of
`SagaStepHandler`s plus a typed context, executed through `SagaTransaction`, which
compensates completed steps in reverse order when a step fails, refuses forward
execution once it has started rolling back, and can resume a crashed saga from a
step log. It also ships a `SqlAlchemySagaStorage` — and that one does not fit this
project:

- its tables are unqualified (`saga_executions`, `saga_logs`), so they would land
  in `public` while every other write table lives in a `write_*` schema;
- their names come from environment variables read **at import time**, which makes
  the schema a property of process start-up rather than of the migration;
- it declares a second declarative base, so its tables cannot be part of the
  Alembic `Base.metadata` that `docs/adr/0003-write-side-outbox.md` and every
  migration already depend on.

There is also no saga lifecycle: the engine persists a status but publishes
nothing, and `SagaMediator.stream` does not surface the dispatched saga's id.

## Decision

**We will split the four process managers by shape, and own the saga tables.**

1. **Orchestration sagas** (`OnboardPlantSaga`, `SpeciesSyncSaga`) are
   `plantkeeper.application.sagas.base.Saga` subclasses — thin subclasses of the
   cqrs `Saga` — whose steps are cqrs `SagaStepHandler`s. Each step commits its own
   transaction through the request's unit of work; the cqrs storage commits the
   saga's state and step log in its own session. That separation is the point: a
   step's business effect must survive the failure of the next step, otherwise
   there would be nothing to compensate.

2. **Choreography sagas** (`AdaptiveWateringSaga`, `MissedCareSaga`) are
   `plantkeeper.application.sagas.consumer.Consumer` subclasses — the write-side
   twin of the read side's `Projection`. They hold no central state. The one piece
   of state a reaction genuinely needs — the window between "watering came due"
   and "the grace period expired" — is a row (`write_care.missed_care_windows`),
   not a process manager.

3. **We will implement `ISagaStorage` ourselves** over
   `write_shared.saga_state` and `write_shared.saga_log`, created by Alembic
   migration `0002`. The execution row carries status, context, an
   optimistic-lock version and a recovery counter; the append-only log carries the
   step transitions the engine replays. The roadmap's single `saga_state(saga_id,
   type, state, step, …)` table is therefore split in two, because compensation has
   to reconstruct *which* steps ran, and a single "current step" column cannot
   answer that.

4. **Saga identity is deterministic**: `uuid5(saga_name, correlation_id)`, where
   the correlation is the plant (onboarding) or the trigger event (species sync).
   A redelivered `PlantAdded` therefore resumes its saga instead of starting a
   second one, and a finished saga is a no-op.

5. **Lifecycle events are the application's job.** `SagaStarted`, `SagaCompleted`,
   `SagaFailed` and `SagaCompensated` are appended to the transactional outbox by
   the saga base, on topic `saga.events`. The engine's status is for the recovery
   job; the events are for everyone else.

6. **Every write-side consumer claims its delivery.** A `(consumer_group,
   event_id)` row in `write_shared.processed_events` is written in the same
   transaction as the work the delivery causes, satisfying `AGENTS.md`'s
   idempotency rule. The read side keeps its own ledger in `read_analytics`
   because Django owns that schema.

7. **The saga base is left unparameterised.** `Saga` extends cqrs's `Saga` without
   a `[Context]` argument. A class inherits `__orig_bases__`, so extending
   `Saga[SagaContext]` makes the library's validator read *that* context for every
   concrete saga and reject its steps. The context type is declared in
   `context_type`, which is what the saga map binds.

## Consequences

**Easier.** The two kinds of process manager can be read and tested on their own
terms: an orchestration saga's whole story is its step list, and a choreography
saga's is its event handlers. Compensation is framework code rather than
project code for the sagas that need it, and the recovery job can resume a crashed
saga because the step log exists. Saga tables are under Alembic like every other
write table, and a failed saga is visible both in `saga_state` and on Kafka.

**Harder.** There is one more adapter to maintain, and it has to implement the
whole `ISagaStorage` protocol including the recovery queries. Saga state is
committed in a session of its own, so a crash between a step's commit and the
log's checkpoint can leave the two disagreeing; the recovery job resolves that by
re-running from the log, and the log is written before the next step starts.
Compensating a step that published an event (`PlantOnboarded`, `SagaCompleted`)
cannot unpublish it: the sequence is ordered so the publish is last, and a
compensated onboarding may leave a stale read-side row until the read model is
rebuilt. Restoring a species during compensation is itself a catalogue change, so
it records a `SpeciesUpdated` rather than rewriting history silently.

**Follow-ups.** Phase 5 adds the telemetry consumer that produces
`TelemetryReceived`, which is what makes `AdaptiveWateringSaga` run for real.
Phase 9 replaces `SpeciesSource` (today a placeholder that returns nothing) and
`SpeciesCache` (today it publishes `SpeciesCacheInvalidated`) with the Trefle
client and the Valkey cache. Nothing in Phase 4 needs to change for either.

## References

- [ROADMAP Phase 4](../ROADMAP.md), [docs/sagas.md](../sagas.md)
- [ADR 0002](0002-bounded-contexts.md) — bounded contexts and the shared kernel
- [ADR 0003](0003-write-side-outbox.md) — why the outbox is this project's own
- [ADR 0004](0004-read-side-projections.md) — the read side's consumer base
- `python-cqrs` 4.13: `cqrs/saga/saga.py`, `cqrs/saga/step.py`,
  `cqrs/saga/storage/protocol.py`, `cqrs/saga/recovery.py`
