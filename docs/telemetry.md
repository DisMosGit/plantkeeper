# Telemetry

> **Status:** Phase 5. The producer is [`tools/iot-simulator`](iot-simulator.md); the
> consumer is `plantkeeper.application.telemetry` running in `apps/workers`. The
> read-side projections for telemetry, and the windowed aggregation over it, are
> Phase 8's `docs/telemetry.md` work and are deliberately not here yet.

## Two topics, two kinds of message

Telemetry is the only flow in PlantKeeper that starts outside the domain, and it is
worth being explicit about how far the raw stream travels:

| Topic | Message | Produced by | Consumed by |
|-------|---------|-------------|-------------|
| `telemetry.raw` | a raw measurement: `sensor_id`, `recorded_at`, `moisture`, `temperature`, `light` | the IoT simulator, directly | the telemetry ingress |
| `telemetry.events` | the `TelemetryReceived` / `SoilMoistureLow` / `SoilMoistureHigh` / `TemperatureAnomaly` / `SensorOffline` domain events | the outbox relay | `AdaptiveWateringSaga`, the read side |

`telemetry.raw` is **not** an event topic. It is not in
`plantkeeper.infrastructure.messaging.topics.EVENT_TOPICS`, nothing in the event
catalogue is ever published to it, and a test asserts it stays that way. A raw
measurement is a fact about a device, not a fact about the business: it has no
`plant_id`, and until the ingress resolves one it has no meaning to any bounded context.

`TelemetryReceived` has no aggregate to raise it — the ingress is its producer — and it
is announced through the outbox relay like any other domain event, so it reaches the
sagas on the same at-least-once, one-writer path as the rest. See
[`docs/events.md`](events.md).

## The raw envelope

```json
{"sensor_id":"…","recorded_at":"2026-09-20T12:00:00+00:00","moisture":42.5,"temperature":21.5,"light":1200.0}
```

- `sensor_id` — a UUID; the key of the Kafka message, so one sensor's readings stay in
  one partition and therefore in order.
- `recorded_at` — an ISO-8601 instant with an offset. Naive timestamps are rejected:
  the reading's instant is part of its identity.
- `moisture` (0–100 %), `temperature` (−50…+60 °C), `light` (≥ 0 lux) — validated
  against the domain's own value objects, so what passes the ingress cannot fail when
  the event is built.

A message that does not validate is logged with a snippet of its body and acknowledged:
retrying a contract violation cannot help, and blocking a partition behind it would
stop every healthy sensor. A reading from a sensor nobody registered is logged and
dropped — the next reading lands as soon as the sensor is registered.

## `write_telemetry.sensor_readings`

Append-only and **RANGE-partitioned by month on `recorded_at`**, because telemetry is
the only high-volume, strictly time-ordered data in the project.

| Column | Type | Note |
|--------|------|------|
| `sensor_id` | `uuid` | the sensor that reported; no foreign key — a reading is a log entry, and deleting a sensor must not rewrite what it reported |
| `recorded_at` | `timestamptz` | when the measurement happened; the partition key |
| `plant_id` | `uuid` | denormalised from the sensor registry when the reading is stored |
| `moisture`, `temperature`, `light` | `double precision` | the measurement |
| `created_at` | `timestamptz` | when the ingress stored it, defaulting to `now()` |

**Primary key `(sensor_id, recorded_at)`.** It is not an arbitrary choice: PostgreSQL
requires every unique constraint of a partitioned table to contain the partition key,
and that pair is exactly the idempotency key the ingress needs. A redelivery, a
`--replay` capture or a second simulator run is absorbed by the database with
`INSERT … ON CONFLICT (sensor_id, recorded_at) DO NOTHING`, which is why the ingress
does not keep a second ledger of its own.

Indexes: `(plant_id, recorded_at)` for "what has this plant's soil been doing", and
`(recorded_at)` for time-range scans. Both are created on the parent and PostgreSQL
propagates them to every partition.

## Partitions

| Where | What it does |
|-------|--------------|
| Alembic `0003` | creates the parent, its indexes and the `sensor_readings_default` catch-all |
| `TelemetryPartitionJob` (worker, daily) | creates the current month plus `TELEMETRY_PARTITION_MONTHS_AHEAD` (3) |
| `plantkeeper.infrastructure.persistence.partitions` | the month arithmetic and the DDL both use |

The catch-all partition is what makes the schedule not load-bearing: **a reading always
has somewhere to land**, even one whose `recorded_at` is outside the created window —
a replayed capture, a badly set clock. The job then does the optimisation, giving each
month its own partition so indexes stay small and a month can be dropped as a unit.

```sql
-- what exists
SELECT child.relname
FROM pg_inherits
JOIN pg_class AS parent ON parent.oid = pg_inherits.inhparent
JOIN pg_class AS child ON child.oid = pg_inherits.inhrelid
WHERE parent.relname = 'sensor_readings';

-- what a month holds
SELECT count(*) FROM write_telemetry.sensor_readings
WHERE recorded_at >= '2026-09-01' AND recorded_at < '2026-10-01';
```

A month created by the job after rows have already fallen into the catch-all partition
fails with "updated partition constraint for default partition would be violated" —
those rows belong to the default now. Create the window ahead of the clock (the job's
default interval is daily, the window is three months) or move the rows first.

## The ingress's transaction

One delivery is one transaction:

1. `INSERT … ON CONFLICT DO NOTHING` the reading;
2. append `TelemetryReceived` to `write_shared.outbox`, but only if the insert was the
   one that stored the row;
3. commit.

A delivery whose row is already there therefore commits nothing and announces nothing:
its event travelled in the transaction that stored the reading, and a second
`TelemetryReceived` would carry a fresh `event_id` that no consumer-group ledger could
recognise as the duplicate it is. A crash before the commit loses nothing (Kafka
redelivers, and the table's key makes the retry a no-op); a crash after it re-inserts
nothing. The consumer group is `plantkeeper-telemetry-ingest`
(`TELEMETRY_INGEST_CONSUMER_GROUP`), and its offset reset is `latest` rather than the
sagas' `earliest`: raw telemetry has no ledger to rebuild against — the readings table
*is* the record — so a fresh group starts at the live edge instead of re-ingesting a log.

## Configuration

| Variable | Default | Meaning |
|----------|---------|---------|
| `TELEMETRY_RAW_TOPIC` | `telemetry.raw` | where the simulator publishes |
| `TELEMETRY_INGEST_CONSUMER_GROUP` | `plantkeeper-telemetry-ingest` | the ingress's consumer group |
| `TELEMETRY_PARTITION_MONTHS_AHEAD` | `3` | how far past the current month the window reaches |
| `TELEMETRY_PARTITION_CHECK_INTERVAL_SECONDS` | `86400` | how often the job looks |

There is no `TELEMETRY_BATCH_SIZE` knob: the ingress stores one delivery's reading per
transaction, because the transaction boundary is what makes the reading and its event
atomic. `SqlAlchemyTelemetryRepository.add_many` still chunks a batch it is handed (500
rows per statement, the bind-parameter ceiling), which a future batched reader can use
without changing the consumer's contract.
