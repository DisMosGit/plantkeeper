# 0008. The outbox pattern, end to end

## Status

Accepted. Complements [ADR 0003](0003-write-side-outbox.md), which records why the
table and the relay are ours rather than `python-cqrs`'s.

## Date

2026-09-21

## Context

[ADR 0003](0003-write-side-outbox.md) settles the *producer* half of reliable
messaging: a hand-rolled `write_shared.outbox`, drained by a relay that owns every
Kafka produce on the write side, with `attempts`, a dead-letter topic and an
idempotency-key table for HTTP replays. It closes with the observation that
at-least-once delivery is "a permanent obligation on every future consumer".

By Phase 10 that obligation has been honoured nine times — six write-side consumers
in `apps/workers`, five projections in `apps/admin` — across two processes, two
databases and two ledgers. The pattern is therefore no longer one table and one loop:
it is the whole delivery story, and a reader who only sees ADR 0003 gets the first
half. This ADR records the second.

Three forces shaped the consumer side as it exists:

1. **`AGENTS.md` requires it.** "Every Kafka consumer must be idempotent on
   `(consumer_group, event_id)`" — a rule, not an option.
2. **Two processes, two databases.** The read side writes Django models in
   `read_analytics`; the write side writes SQLAlchemy models in `write_*`. Neither
   may import the other, and each owns its own ledger because a ledger shared across
   databases would be the very distributed transaction the outbox exists to avoid.
3. **At-least-once is chosen, not tolerated.** ADR 0003 picks it deliberately: a
   duplicate is recoverable, a loss is not. So every consumer must be safe to run
   twice with the same message, including after an offset reset.

## Decision

We will treat reliable delivery as **one pattern with two halves**, and every consumer
in the platform implements the consuming half the same way.

**The producing half** (ADR 0003, restated here so the pattern reads in full):
`SqlAlchemyUnitOfWork.commit()` inserts the aggregate's recorded events into
`write_shared.outbox` inside the aggregate's own transaction; `OutboxRelay` polls
that table with its own session, publishes each row, commits per row, and after
`outbox_max_attempts` failed polls copies the row to `plantkeeper.dlq.v1` with
`original_topic` and `error` headers. The relay is the **only** producer on the write
side; no router, handler or repository may call the broker.

**The consuming half.** Every consumer that has a ledger — a write-side `Consumer`,
a saga trigger, a read-side `Projection` — claims the delivery before doing any
work, *in the same transaction as the work* (the telemetry ingress keeps no ledger;
its guard is the readings key, below):

- `write_shared.processed_events(consumer_group, event_id)` for the worker;
- `read_analytics.processed_events(consumer_group, event_id)` for the read side
  (owned by Django, because the read schema is).

A second delivery finds the row and stops. A failure normally rolls the claim back
with the work, so the retry starts from nothing — the exception being a saga trigger
that recorded its saga's failure: `SagaFailed` is committed with the claim and the
`failed` saga is not retried (ADR 0005, `docs/sagas.md`). The ledger key is
`(consumer_group, event_id)`
and never `event_id` alone: a rebuilt consumer group with a lost ledger is a real
scenario, and two consumers of the same event are the normal case.

**A second guard where the ledger cannot reach.** A consumer whose group is recreated
(replayed from `earliest` with an empty ledger) still sees every message again. The
three consumers whose *effects* must be unique regardless derive their row's identity
from the event instead of from a sequence:

- `JournalEntryConsumer` derives the entry id from the event
  (`uuid5` of plant and completion moment) and treats an existing entry as a no-op;
- `NotificationConsumer` does the same for the notification's id, plus a guard that
  at most one unread `soil_moisture_low`/`temperature_anomaly` exists per plant (a
  reading every ten seconds would otherwise be a stream of reminders);
- the telemetry ingress is guarded by the readings table's primary key
  `(sensor_id, recorded_at)`, and appends `TelemetryReceived` **only** when its insert
  actually inserted — a redelivery that stores nothing announces nothing, which is
  what keeps a second event with a fresh `event_id` out of the stream.

**Offsets follow from the ledger.** A new consumer group starts at `earliest`,
because the ledger makes a full replay safe and is what allows a read model or a
saga's state to be rebuilt from its topic. The telemetry ingress is the one exception
(`latest`): raw telemetry has no ledger of its own — the readings table is its record
— so replaying an old log would re-insert rows nobody asked for.

## Consequences

- **Easier:** "is this consumer safe?" has one answer, in one place, for every
  consumer. The claim, the work and the offset are three views of one fact.
- **Easier:** a read model can be rebuilt by dropping its consumer group and the
  ledger rows for it. `docs/cqrs.md` documents that as the recovery procedure, and it
  works precisely because duplicates are no-ops.
- **Harder:** every consumer carries a ledger row per delivery. Both ledgers grow
  without bound; pruning them is a runbook concern, not a correctness one, because a
  deleted row only re-opens the door to one replay.
- **Harder:** a producer that wants to publish directly must not. The rule in ADR 0003
  is now a platform rule: the outbox is the only produce path, and a new producer
  means a new outbox append, not a broker call.
- **Constrained:** the two ledgers cannot be merged, and `apps/admin` may not import
  `apps/workers` (the layer contract). The *pattern* is shared — decoding
  (`plantkeeper.infrastructure.messaging.decoding`), the claim protocol and the
  earliest-offset policy — while each side keeps its own table.
- **Follow-up:** `SensorOffline` still has no producer (it needs a silence timer, not
  a delivery guarantee); `write_shared.idempotency_keys` still grows without expiry.
  Both are listed in `docs/events.md` and `docs/cqrs.md` as known limits.

## References

- [`docs/adr/0003-write-side-outbox.md`](0003-write-side-outbox.md) — the table, the
  relay, the DLQ, and the field-by-field comparison with `python-cqrs`'s outbox
- [`docs/events.md`](../events.md) — the message contract, at-least-once, the DLQ and
  each event's consumer group
- [`docs/cqrs.md`](../cqrs.md) — the read side's ledger and how to rebuild a read model
- [`packages/infrastructure/src/plantkeeper/infrastructure/messaging/relay.py`](../../packages/infrastructure/src/plantkeeper/infrastructure/messaging/relay.py)
  and [`.../messaging/decoding.py`](../../packages/infrastructure/src/plantkeeper/infrastructure/messaging/decoding.py)
- [`packages/application/src/plantkeeper/application/sagas/consumer.py`](../../packages/application/src/plantkeeper/application/sagas/consumer.py)
  — `consume_once`, the claim shared by every write-side consumer
- [microservices.io — Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html)
  and [Idempotent consumer](https://microservices.io/patterns/communication-style/idempotent-consumer.html)
