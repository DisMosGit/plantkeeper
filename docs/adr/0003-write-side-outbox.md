# 3. Transactional outbox and the write side

## Status

Accepted (Phase 2)

## Date

2026-09-20

## Context

Phase 2 has to make one promise: a command that returns `201 Created` has both
persisted the aggregate **and** arranged for its event to reach Kafka. A database
commit and a Kafka produce are two independent systems, so the naive orderings
are both wrong:

- **publish, then commit** — a crash in between leaves consumers reacting to a
  plant that does not exist, and the event cannot be unpublished;
- **commit, then publish** — a crash in between loses the event forever, and no
  consumer ever learns about a plant that does exist.

The two failure modes are not symmetric. A lost `PlantAdded` is permanent
divergence between contexts with no trace to reconcile from; a duplicate
`PlantAdded` is a nuisance a consumer can defend against. So the system must
choose at-least-once semantics, and it must make the "event will eventually be
published" fact part of the same transaction as the aggregate.

Three further constraints shaped the answer:

1. **`python-cqrs` is already a dependency** and ships an outbox abstraction.
   Whether to use it or to own the table had to be decided on evidence, not
   preference.
2. **Every message needs the same contract**: the event's own JSON document as
   the body, `event_name` and `event_id` in the headers, and an aggregate-derived
   Kafka key so one plant's events stay ordered.
3. **The relay must be restartable and observable.** An operator has to be able
   to ask "what is stuck?" and "why?" from the database alone, and a poison
   message must not block the queue or be dropped silently.

## Decision

We will implement a **hand-rolled transactional outbox** in the `write_shared`
schema and a relay that owns every Kafka produce on the write side.

**The table.** `write_shared.outbox` carries `id` (identity, the drain order),
`event_id` (unique), `event_name`, `topic`, `partition_key`, `payload` (`jsonb`),
`occurred_at`, `created_at`, `published_at`, `dead_lettered_at`, `attempts` and
`last_error`. The partial index `ix_outbox_unpublished` covers `id` where
`published_at IS NULL AND dead_lettered_at IS NULL`: exactly the relay's query,
without indexing the rows that are finished.

**The write.** `SqlAlchemyUnitOfWork.commit()` drains the events the transaction's
aggregates recorded, resolves each one's topic and partition key, inserts one
outbox row per event, and commits aggregates and outbox rows **in one
transaction**. If the insert fails the aggregate write fails with it. A topic is
resolved at append time; an event with no topic mapping raises rather than being
published to a guess.

**The relay.** `OutboxRelay` polls with its own session — never a request's, which
would read rows that are not committed yet — and publishes with a FastStream
`KafkaBroker`. Each message is committed on its own, so one poison message
cannot hold back the batch. Failure handling has three layers: `tenacity` smooths
a transient broker blip inside one poll; the `attempts` column counts *failed
polls*, so a restart does not reset the count; and after `outbox_max_attempts`
(5) the message is copied to `plantkeeper.dlq.v1` with `original_topic` and
`error` headers and the row is marked `dead_lettered_at`. If the dead-letter copy
itself fails the row is counted as a failed attempt instead, so a message is
never dropped silently. The poll delay backs off exponentially while publishing
keeps failing, and it waits on the stop event so shutdown is immediate.

**Idempotency.** `write_shared.idempotency_keys(key PRIMARY KEY, request_hash,
status_code, response, created_at)` stores the response of a create command. The
winning insert makes a concurrent duplicate fail on the primary key; the loser
then reads the winner's stored response and replays it. The same key with a
different body is a `409 IdempotencyKeyConflictError`.

**Why not `python-cqrs`'s outbox.** Its model has no `published_at`, no
`attempts` and no dead-letter marker, so it cannot express "unpublished", "how
many times has this failed" or "stop retrying this one" — the three questions the
relay is built around. It also ships no relay, and its event abstractions would
require a per-event adapter plus a global registry to map a domain event to a
topic. What `python-cqrs` genuinely provides — `PydanticRequest` /
`PydanticResponse`, `RequestHandler`, `RequestMap`, `RequestMediator` and the
Dishka container — is used as-is.

## Consequences

- **Easier:** atomicity is a schema property, not a convention. A review can point
  at the single `commit()` that inserts aggregates and outbox rows together.
- **Easier:** delivery is debuggable from SQL. Stuck messages are rows with a
  non-null `last_error`; exhausted ones are rows with `dead_lettered_at`.
- **Easier:** the message contract lives in one place
  (`infrastructure/messaging/topics.py` and `publisher.py`), so every context
  publishes the same envelope-less shape.
- **Harder:** `attempts` counts failed *polls*, not individual produce calls, so
  a message retried three times by `tenacity` inside one poll increments the
  column once. The dead-letter threshold is therefore a threshold on polls; this
  is deliberate, because it is the unit that survives a restart.
- **Harder:** delivery is at-least-once, so every consumer must deduplicate on
  `(consumer_group, event_id)`. This is a permanent obligation on every future
  consumer, not an implementation detail of the relay.
- **Harder:** one relay process is a single point of delay, though not of
  correctness — the batch query is safe to run from two relays because
  `mark_published` is idempotent, but two relays would duplicate messages.
- **Constrained:** nothing on the write side may publish to Kafka directly. A
  router, handler or repository that calls the broker bypasses the outbox and
  loses the guarantee; the relay is the only producer.
- **Follow-up:** `write_shared.idempotency_keys` grows without bound until Phase
  8 adds expiry; `POST /api/v1/households` exists because `POST /api/v1/plants`
  needs a household and Identity has no ORM model yet.
- **Superseded-in-part:** the Phase 10 roadmap lists a planned ADR
  `outbox-pattern`; it now has to either reference this one or replace it when
  the pattern is documented for the whole platform.

## References

- [`docs/events.md`](../events.md) — event catalogue, topics, headers and keys
- [`docs/architecture.md`](../architecture.md) — layers and the write path
- [`docs/adr/0002-bounded-contexts.md`](0002-bounded-contexts.md) — why events, not
  direct calls, are the only channel between contexts
- [`packages/infrastructure/src/plantkeeper/infrastructure/messaging/relay.py`](../../packages/infrastructure/src/plantkeeper/infrastructure/messaging/relay.py)
  — the relay
- [microservices.io — Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html)
