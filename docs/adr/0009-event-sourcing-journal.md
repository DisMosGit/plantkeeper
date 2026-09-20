# 0009. Event sourcing for the Journal

## Status

Accepted

## Date

2026-09-21

## Context

Six of the seven bounded contexts store current state and publish a fact as a side
effect: a `Plant` is a row, and `PlantAdded` is what happened to it. The Journal is
the odd one out on purpose. A care log is read as *history* — "what did this plant's
care look like before the holiday", "when was it last repotted" — and a history that
is stored as mutable rows loses the very thing it is for the moment somebody edits a
row. It also has a second reader: the care facts arrive as events from Care, so
recording them by replaying the events that caused them is one move rather than a
translation.

The roadmap (§6.1–6.4) asks for an append-only store, an aggregate that restores from
it, snapshots, a REST surface and Django Admin. The decisions that were not in the
roadmap are the ones this ADR records:

1. whether event sourcing is a project-wide choice or a per-context one;
2. how a stream detects and refuses a lost race;
3. what a snapshot means for an append-only log, and when it is worth taking;
4. what to do when a stream does not replay — a corruption, not a contract violation;
5. whether the Phase 2 tabular `journal_entries` table is removed.

## Decision

**Event sourcing is per-context, and the Journal is the only context that uses it.**
The other six keep SQLAlchemy tables and publish facts. The reason is cost, not
taste: event sourcing turns every read into a replay, every schema change into a
versioning problem and every repair into an appended event, and that is worth paying
where the history *is* the product. A plant's current name and location are not.

**One stream per plant; the stream id is the plant id.** `write_journal.event_store`
has one row per fact, keyed by `(stream_id, version)` with a unique `event_id`, a
`global_position` identity column for a deterministic `load_all`, and the event
document in `jsonb` — the same shape the outbox carries, so an operator reads one
format in both places.

**The optimistic lock is the unique key, not a lock statement.** An appending
transaction replays the stream, computes `expected_version + 1` and inserts; a lost
race produces an `IntegrityError`, translated into `EventStoreConcurrencyError` — a
`ConcurrentWriteError`, which the REST and gRPC layers map to `409`/`ABORTED`. There
is no `SELECT … FOR UPDATE` and no version column on the aggregate, so concurrency is
a property of the schema rather than of a calling convention.

**A snapshot bounds the replay; it does not summarise it.** The aggregate is
append-only and immutable, so its state *is* its entries. `journal_snapshots` stores
that list every `SNAPSHOT_EVERY = 50` events, and `load_stream` starts from the latest
snapshot's version and tail-reading the events after it. The gain is how many rows a
replay deserialises, not a compressed state — and the interval is a module constant,
not a setting, because it is a Journal policy rather than a property of the
environment. Snapshot retention and pruning are explicitly out of scope.

**A stream that does not replay is an error, not a skip.** `EventStoreCorruptionError`
(HTTP 500) is raised for a gap in the version sequence, an unknown `event_type`, or a
row that is not a journal event. This is deliberately *stricter* than the Kafka
decoder's policy, which logs an unknown `event_name` and acknowledges it: a topic is
shared and one bad message must not block a partition, while a local stream is this
aggregate's only source of truth and a partial replay would produce a confidently
wrong answer.

**`write_journal.journal_entries` stays, written in the same transaction as the
append.** It is not a second source of truth: the appending code is its only writer,
so it cannot diverge, and dropping a table is a separate, destructive decision with no
benefit in this phase. Nothing reads it for a response.

**Idempotency is doubled for the consumer.** `JournalEntryConsumer` claims
`(consumer_group, event_id)` in `write_shared.processed_events` like every other
consumer, *and* derives `entry_id` from the event, so a group rebuilt with an empty
ledger appends nothing. An append-only history does not survive a duplicate.

## Consequences

- **Easier:** `GET /api/v1/journal/{plant_id}/at?date=…` is a replay, not a feature.
  It keeps entries whose care moment is at or before the end of that UTC day, which is
  the question the log is asked.
- **Easier:** the read side and the write side tell the same story: the event that
  moved the schedule is the event that becomes a journal entry, and the projector is
  the same event the aggregate appended.
- **Harder:** the replay is a versioned contract. A change to a journal event type
  needs a migration path, and `event_type` is checked against the catalogue on every
  replay for exactly that reason.
- **Harder:** two writers to one stream are possible — the consumer and the command —
  and a burst of care events for one plant serialises on the version. The alternative
  (per-writer streams) would break the ordered-log invariant the timeline depends on.
- **Constrained:** `AddJournalEntryCommand` has no REST endpoint. The roadmap's §6
  does not ask for one, the consumer is its caller in production, and a manual entry
  endpoint would need an authorization story the project does not have.
- **Constrained:** `FERTILIZING`, `REPOTTING` and `NOTE` entries have no producer yet:
  Care produces no event for them today. `docs/event-sourcing.md` records it, and the
  aggregate accepts them so the endpoint or producer can be added without a migration.
- **Follow-up:** `load_all` is implemented and tested but has no production caller; it
  is the entry point for a future replay/rebuild tool.

## References

- [`docs/event-sourcing.md`](../event-sourcing.md) — the stream table, the write path,
  the replay, the snapshot policy and the REST surface in full
- [`docs/events.md`](../events.md) — `JournalEntryAdded` and its transport
- [`docs/adr/0002-bounded-contexts.md`](0002-bounded-contexts.md) — why the Journal is
  its own context
- [`packages/domain/src/plantkeeper/domain/journal/`](../../packages/domain/src/plantkeeper/domain/journal/)
  — `JournalEntry`, `JournalStream`, `JournalAggregate`
- [`packages/application/src/plantkeeper/application/journal/store.py`](../../packages/application/src/plantkeeper/application/journal/store.py)
  — the one append path and the snapshot-aware replay
- [`packages/infrastructure/src/plantkeeper/infrastructure/persistence/repositories/event_store.py`](../../packages/infrastructure/src/plantkeeper/infrastructure/persistence/repositories/event_store.py)
  — the store, the optimistic lock and the corruption checks
