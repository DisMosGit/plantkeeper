# Event sourcing: the Journal

> **Status:** Phase 6. The journal is append-only, replayable from its own stream, and
> can answer "what did it look like on any date". The roadmap reserves a dedicated ADR
> for this phase (`0009-event-sourcing-journal.md`, Phase 10); the decisions taken here
> are recorded below and in the Phase 6 close-out in `ROADMAP.md`.

The Journal is the project's **only** event-sourced aggregate. Every other context
stores current state and publishes facts as a side effect; the journal stores the
facts and derives state from them, which is what makes "roll the journal back to a
date" a query rather than a feature nobody remembered to build.

## Why only the Journal

Event sourcing is a storage decision with a cost: every read becomes a replay, every
schema change becomes a versioning problem, and every "just fix this row" becomes an
event. It pays for itself where the *history itself* is the product — a care log that
a household reads back and that must never silently lose a fact. The plant, the
schedule and the sensor are all "what is true now", so they stay ordinary tables
managed by SQLAlchemy.

`packages/domain` has the aggregate model: `JournalEntry` is one immutable fact,
`JournalStream` is one plant's ordered entries, and
`JournalAggregate` is the stream replayed into state.

## The stream

One stream per plant; the stream id **is** the plant id.

| Column | Type | Why |
|---|---|---|
| `global_position` | `bigint identity`, PK | The store's own append order across every stream, so `load_all` has a deterministic order without a timestamp tie-break |
| `stream_id` | `uuid` | The plant id |
| `version` | `integer`, `> 0` | Position in the stream; assigned by the appending transaction |
| `event_id` | `uuid`, unique | The domain event's own id; appending it twice cannot yield two rows |
| `event_type` | `varchar(255)` | The class name — the same vocabulary the Kafka `event_name` header uses |
| `payload` | `jsonb` | The event document, byte-for-byte the shape the outbox carries |
| `occurred_at` | `timestamptz` | When the event happened |
| `created_at` | `timestamptz` | When it was appended |

`UNIQUE (stream_id, version)` is the whole concurrency design. There is no
`SELECT … FOR UPDATE` and no version column on the aggregate: an appending transaction
inserts at `expected_version + 1` and flushes immediately, so the second writer of a
version gets an `IntegrityError` translated into `EventStoreConcurrencyError`, the unit
of work rolls back, and nothing half-written survives. A consumer that loses the race
is redelivered (its ledger claim rolled back with the transaction); a command answers
409.

Tables live in `write_journal`, created by Alembic: `event_store` (0004) and
`journal_snapshots` (0005). `write_journal.journal_entries` — the tabular table from
Phase 2 — is kept in the same transaction as the append. It is the write side's own
record of the same facts and is not on any read path; the appending code is the only
writer, so it cannot drift.

## Writing

`plantkeeper.application.journal.store.record_journal_entry` is the one append path,
shared by `AddJournalEntryHandler` and `JournalEntryConsumer`. In one transaction it:

1. replays the stream into a `JournalAggregate`;
2. calls `aggregate.add_entry(...)`, which records exactly one `JournalEntryAdded`;
3. appends the event to `event_store` at `expected_version + 1`;
4. appends the same event to the **outbox**, so the fact and its publication are atomic
   (the event store is not an aggregate repository, so the outbox row is written
   explicitly — the same shape the telemetry ingress uses for `TelemetryReceived`);
5. writes the tabular mirror row;
6. checkpoints the state when the version is a multiple of `SNAPSHOT_EVERY` (50).

It never commits: `AddJournalEntryHandler` wraps it in its own transaction, and the
Kafka consumer runs it inside the ledger's.

### Who writes to it

`JournalEntryConsumer` (`plantkeeper.application.journal.consumer`) turns
`WateringCompleted` into a `WATERING` entry whose care moment is the event's
`completed_at`. It is registered in the worker's consumer registry like the
choreography sagas and gets its own group (`<prefix>-journal-entries`).

**Idempotency is defended twice.** The ledger claims `(consumer_group, event_id)` in
the same transaction as the append, as every consumer does; and the entry's identifier
is derived from the event (`watering_entry_id`), so even a rebuilt group — a reset
offset plus a lost ledger — finds the entry already in the stream and appends nothing.
An append-only history cannot afford the duplicate that the ledger alone would let
through.

`AddJournalEntryCommand` carries the same path for any entry kind, with an optional
care moment, note and entry id. It has no HTTP route yet: a manual "fertilized today"
endpoint is a later phase's concern, and `FERTILIZING`, `REPOTTING` and `NOTE` entries
have no care producer today.

## Reading: replay and snapshots

`load_journal_aggregate` is the one read path:

1. ask `JournalSnapshotRepository.latest(plant_id)` for the newest checkpoint;
2. read the stream **after** the checkpoint's version;
3. start from the checkpoint's state (or an empty aggregate) and `apply` each event.

The versions are checked for continuity while replaying. A gap, an unknown
`event_type`, a payload that no longer validates, or a foreign event in a journal
stream raises `EventStoreCorruptionError` — unlike a Kafka delivery, a stored stream is
local and has to be fully replayable, so it is never "skipped with a warning".

### Snapshots

`write_journal.journal_snapshots` is keyed by `(stream_id, version)` and holds the
serialised `JournalState` (the plant, the version, and the ordered entries). A reader
takes the highest version. The row is written every `SNAPSHOT_EVERY` events, which is a
module constant rather than a setting because it is a policy of the journal, like the
sagas' grace period.

An append-only aggregate's state *is* its entry list, so a snapshot does not shrink the
state — it bounds how many event rows a replay deserialises. Retention and pruning are
deliberately out of scope: a stream keeps its checkpoint history, which is useful for
debugging, and a household journal is small.

## Asking "what did it look like on that date"

`GET /api/v1/journal/{plant_id}` replays the whole stream;
`GET /api/v1/journal/{plant_id}/at?date=YYYY-MM-DD` replays it and keeps the entries
whose **care moment** (`entry.occurred_at`, in the domain's stream ordering) is at or
before the end of that UTC day, inclusive.

The care moment is the axis, not the moment the entry was recorded, because a care log
answers "what had we done by then?" — and that is also how the journal orders itself
and how the admin timeline shows it. The recorded moment is per-entry data
(`JournalEntryAdded.occurred_at`), not the query's axis: a backdated entry added later
still belongs to the day it happened. A bare date means the last moment of that day
(`time.max`, UTC).

## Deliberate gaps

- **Skips and misses are not journaled.** `CareSkipped`, `CareMissed` and
  `WateringRescheduled` record that nothing was done or that a plan moved; the journal
  records what happened.
- **No manual-entry endpoint.** `AddJournalEntryCommand` exists and is tested; exposing
  it as `POST /api/v1/journal/{plant_id}` is a later phase.
- **A stream is not partitioned or capped.** A plant's journal is small and read whole.
- **The store is journal-shaped.** `event_store` lives in `write_journal` and the port
  hands back `JournalEntryAdded` streams; a second event-sourced context would justify
  promoting a generic store.

## Operating it

```bash
# the whole history of one plant, oldest first
docker exec plantkeeper-postgres psql -U plantkeeper -d plantkeeper -c \
  "SELECT version, event_type, occurred_at FROM write_journal.event_store \
   WHERE stream_id = '<plant_id>' ORDER BY version"

# the newest checkpoint of every stream
docker exec plantkeeper-postgres psql -U plantkeeper -d plantkeeper -c \
  "SELECT DISTINCT ON (stream_id) stream_id, version FROM write_journal.journal_snapshots \
   ORDER BY stream_id, version DESC"

# what the replay would read
curl 'localhost:8000/api/v1/journal/<plant_id>'
curl 'localhost:8000/api/v1/journal/<plant_id>/at?date=2026-09-20'
```

A `load_all(after_position, limit)` read is implemented and tested as the hook a
replay/rebuild tool would use; nothing on the write path calls it yet.

## Tests

- `tests/unit/domain/test_journal_aggregate.py` — appending, replay, the temporal
  state and the snapshot policy.
- `tests/unit/application/test_journal_event_sourcing.py` — the append path and the
  replay path against an in-memory store, including the snapshot-only tail read and the
  corruption errors.
- `tests/unit/infrastructure/test_mappers.py` — event ↔ row and state ↔ checkpoint.
- `tests/integration/test_event_store.py` — Postgres: version assignment, the optimistic
  lock, `load_all`, snapshots and the automatic checkpoint.
- `tests/integration/test_consumers.py` — `JournalEntryConsumer`, a redelivery, and a
  rebuilt group.
- `tests/e2e/test_journal_flow.py` — watering over HTTP → Kafka → journal, then the
  dated replay through the API.

## References

- [`docs/events.md`](events.md) — `JournalEntryAdded` and the topic it travels on.
- [`docs/cqrs.md`](cqrs.md) — the read side's `JournalProjection`.
- [`docs/adr/0003-write-side-outbox.md`](adr/0003-write-side-outbox.md) — why the
  outbox row is written in the same transaction as the fact.
