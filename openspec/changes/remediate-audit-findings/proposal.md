# Proposal

## Why

An architecture audit of the platform found three classes of problem that a senior
reviewer would hold against it. **Correctness gaps:** the saga engine commits its
state in a different transaction from the step's business write and its outbox rows,
the outbox relay neither claims rows nor preserves per-key order when a message
fails, a consumer that raises has neither a bounded retry nor a dead-letter path,
and a recorded saga failure is terminal even when the cause was transient.
**Overstated claims:** the README says contexts "communicate only through Kafka
events" while sagas write other contexts's tables and two consumers read Garden
tables; it calls the read side "CQRS" while every client query is answered from the
write store. **Missing pieces:** the platform's highest-volume stream (telemetry)
has no query path at all, `SensorOffline` is catalogued but can never fire, and the
event envelope carries no correlation, causation, actor or schema version. This
change applies the full remediation roadmap the audit produced.

## What Changes

- **Saga state and effects become one transaction (P0).** A step's business write,
  its `saga_log` entry, its `saga_state` checkpoint and its lifecycle event commit
  together in the unit of work, closing the crash window between "state says done"
  and "effect exists".
- **Cross-context saga effects become recorded commands (P1).** A step that changes
  another context records a command intent in the same transaction as the process
  state; a dispatcher consumer executes each intent through that context's own
  handlers. A process manager no longer writes another context's tables directly.
  **BREAKING** for `saga_state` consumers: a new `write_shared.saga_intents` table
  and a new consumer group appear.
- **Recorded saga failures become retryable.** A failed process is retried up to a
  bounded budget before it is parked for an operator, and an operator can reset and
  re-dispatch it. **BREAKING**: the current "a recorded failure is final" rule
  changes.
- **The event envelope gains provenance and versioning.** Headers and document gain
  `correlation_id`, `causation_id`, `raised_by`, `schema_version` and W3C
  `traceparent`, propagated from an API request or a trigger through every saga
  step and derived event. **BREAKING** for hand-written producers that send the
  old two-header envelope only if the new fields are made mandatory for new events;
  additive for existing consumers.
- **The outbox relay becomes multi-instance safe and order-preserving.** Rows are
  claimed with `FOR UPDATE SKIP LOCKED` and a lease, and a message that fails stops
  the later messages of the same partition key until it succeeds or is abandoned.
- **Consumer failures get a terminal path.** A handler error is retried a bounded
  number of times with backoff and then dead-lettered with its claim recorded,
  closing the gap `event-transport` currently records as "the error propagates and
  the broker redelivers it".
- **Cross-context reads are replaced with event-carried state transfer.** A context
  that needs a stable reference to another context's aggregate keeps its own
  reference row, maintained from that context's events, instead of reading the other
  context's tables.
- **Client queries move to the read side (CQRS).** List and report queries are
  answered from read models through a read-side query port; command-support reads
  and read-your-writes answers stay on the write store. The read side runs on its
  own database instance. **BREAKING** for deployment topology: `make dev` starts two
  databases and `make migrate` applies both.
- **Telemetry gains a read side.** A dedicated `read_telemetry` schema holds
  five-minute rollups and a per-sensor latest row, projected from `telemetry.events`
  by its own consumer group; raw readings gain a retention window. This reverses the
  two recorded decisions "telemetry facts reach no read model" and "a silent sensor
  is not announced".
- **A silent sensor is announced.** The sensor's `last_seen_at` is persisted and a
  silence timer publishes `SensorOffline` at most once per silence, making the
  catalogued event real.
- **Notifications gain a streaming endpoint.** Server-Sent Events is the primary
  delivery channel over the same payload-free household signal, with the
  request/wait endpoint kept as a fallback; waiting holds no database session.
- **The written architecture matches the code.** README and `docs/architecture.md`
  claims are corrected (integration is events *between processes*; sagas coordinate
  in-process through the shared application layer; the query split is stated), and
  an ADR records the trust model: no authentication, the accepted risks, and the
  envelope groundwork that makes adding it later non-breaking.

## Capabilities

### New Capabilities

None. Every behaviour lands in a capability that already exists.

### Modified Capabilities

- `process-managers`: step effects, step history and lifecycle events commit in one
  transaction; cross-context effects are recorded commands dispatched by a consumer;
  a recorded failure is retried within a bounded budget instead of being final, and
  an operator can reset a parked process.
- `event-transport`: the envelope carries correlation, causation, actor, schema
  version and trace context; the relay claims rows so it can run in multiple
  instances and preserves per-key order across failures; a consumer that keeps
  failing is dead-lettered with its claim rather than redelivered forever; a context
  keeps event-maintained reference rows instead of reading another context's tables.
- `read-models`: client list and report queries are answered from read models while
  command-support reads stay on the write side; the read side runs on its own
  database; the list of deliberately unprojected events shrinks because telemetry is
  now projected.
- `notifications`: delivery offers a server-sent event stream as the primary channel
  and keeps the request/wait form as a fallback; a waiting request holds no database
  session.
- `iot-telemetry`: telemetry facts reach a read side of rollups and a latest row in
  a dedicated read schema; raw readings have a retention window; a silence timer
  publishes the offline event, reversing "a silent sensor is not announced".
- `developer-workflow`: `make dev` starts the broker, two databases and the cache;
  the workers target additionally runs the command dispatcher and the sensor silence
  timer.

## Impact

- **Domain** (`packages/domain`): the event envelope gains provenance fields; the
  sensor aggregate persists `last_seen_at` and can raise `SensorOffline`.
- **Application** (`packages/application`): saga base and step handlers record
  command intents; a command-dispatcher use case; a read-model query port and the
  list/report query handlers move to it; correlation propagates through handlers.
- **Infrastructure** (`packages/infrastructure`): `saga_intents` table and Alembic
  migration; saga storage joins the unit of work's transaction; relay claiming and
  per-key ordering; consumer retry/dead-letter policy; a second database engine for
  the read side; the silence timer job.
- **Apps**: `apps/workers` registers the dispatcher consumer and the silence timer;
  `apps/admin` gains telemetry projections and `read_telemetry` models; `apps/api`
  gains the SSE endpoint and routes list/report queries to the read port.
- **Topology**: `docker-compose.yml` runs a second Postgres for the read side;
  `make migrate` applies write (Alembic) and read (Django) schemas to their own
  instances.
- **Contracts and docs**: OpenAPI, both AsyncAPI documents and the generated
  diagrams change with the envelope and the new consumer; `docs/events.md`,
  `docs/cqrs.md`, `docs/sagas.md`, `docs/notifications.md`, `docs/telemetry.md`,
  README and `docs/architecture.md` are updated; one new ADR records the trust
  model and one records the saga command dispatch.
- **Tests**: unit tests for the envelope, saga intents and retry policy; integration
  tests for relay claiming/ordering, the dispatcher, telemetry rollups and the
  silence timer; end-to-end tests for the SSE stream and the client-query routing.
- **Explicitly out of scope**: authentication (forbidden by `developer-workflow` and
  AGENTS.md; the change only prepares the schema for it), Kubernetes/CI/Terraform,
  and any new broker or task queue.
