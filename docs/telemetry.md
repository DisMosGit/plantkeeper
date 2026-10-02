# Telemetry

> **Status:** Phase 5, extended in Phase 8 and in the audit remediation. The producer
> is [`tools/iot-simulator`](iot-simulator.md); the consumer is
> `plantkeeper.application.telemetry` running in `apps/workers`. The ingress calls
> `Sensor.record`, so the threshold events have a real producer and feed the
> notifications of [`docs/notifications.md`](notifications.md), and it persists the
> sensor's silence state, so `SensorOffline` has a producer too — the silence timer in
> the worker. `read_telemetry` is the telemetry read side: the rollup projection
> consumes `telemetry.events` and answers "how has this plant been doing" without
> touching the raw readings.

## Two topics, two kinds of message

Telemetry is the only flow in PlantKeeper that starts outside the domain, and it is
worth being explicit about how far the raw stream travels:

| Topic | Message | Produced by | Consumed by |
|-------|---------|-------------|-------------|
| `telemetry.raw` | a raw measurement: `sensor_id`, `recorded_at`, `moisture`, `temperature`, `light` | the IoT simulator, directly | the telemetry ingress |
| `telemetry.events` | the `TelemetryReceived` / `SoilMoistureLow` / `SoilMoistureHigh` / `TemperatureAnomaly` / `SensorOffline` domain events | the outbox relay | `AdaptiveWateringSaga`, `NotificationConsumer`, the telemetry rollup projection |

`read_telemetry` is the telemetry read side, and it is a schema of its own rather than
a corner of `read_analytics`: the rollups are the platform's highest-volume read model,
they are rebuilt on a schedule of their own, and keeping them apart leaves the domain
read models' shape, one-writer rule and rebuild story untouched. Nothing a sensor
reports is copied into `read_analytics` — the raw readings stay the write side's detail
record.

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
| `TelemetryPartitionJob` (worker, daily) | creates the current month plus `TELEMETRY_PARTITION_MONTHS_AHEAD` (3), and drops every month beyond `TELEMETRY_RETENTION_MONTHS` (12) |
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

### Retention

Raw readings are kept only for a documented window — `TELEMETRY_RETENTION_MONTHS`,
twelve months by default — and the same job enforces it by dropping the partition of
every month that lies **entirely** beyond it. A month that overlaps the window stays,
so no partially-needed data is ever dropped; the catch-all partition is never a
candidate, because it holds the readings whose month never had a partition of its own.

Dropping a partition is one `DROP TABLE` rather than a `DELETE` of everything in it,
which is the reason the table is partitioned by month at all. Two consequences are
worth stating plainly:

* **the rollups outlive the readings.** `read_telemetry` lives on the read instance and
  keeps the per-window aggregates — min/mean/max and a sample count — so "how has this
  plant been doing since spring" still answers after the raw rows behind it are gone.
  It is the reason the retention window can be short without losing the history a
  household cares about.
* **the limit is documented, not silent.** A reading whose month has already been
  dropped lands in the catch-all partition and is stored: retention is a maintenance
  policy, never a refused write.

## The telemetry read side (`read_telemetry`)

Telemetry is projected by `TelemetryRollupProjection` in `apps/admin`, under a consumer
group of its own (`<prefix>-telemetry-rollup`, `READ_SIDE_CONSUMER_GROUP_PREFIX`) with
its own ledger rows. It consumes `telemetry.events` — the *domain* facts the ingress
published after validating and resolving the raw stream — rather than `telemetry.raw`,
so it never repeats the ingress's validation, sensor-to-plant resolution or
deduplication.

| Table | One row per | Keeps |
|-------|-------------|-------|
| `telemetry_rollups` | sensor × fixed window (one hour, UTC) | `moisture_min/avg/max`, `temperature_min/avg/max`, `sample_count` |
| `sensor_latest` | sensor | the newest reading, the newest threshold alert, and the last silence announcement |

**A window is recomputed, never duplicated.** The upsert inserts a one-sample rollup
and folds it into the existing row `ON CONFLICT (sensor_id, bucket_start)`, deriving the
new mean from the stored mean and count and taking `LEAST`/`GREATEST` for the extremes.
That makes the aggregate exact whichever order the readings arrive in, which matters
because the relay only promises per-key order and a replay can deliver a month in any
shape. `sensor_latest` moves forward only: an older reading is stored and rolled up, but
it never walks "latest" backwards.

The three threshold alerts (`SoilMoistureLow`, `SoilMoistureHigh`,
`TemperatureAnomaly`) are recorded on `sensor_latest.last_alert_*` rather than in the
rollup: the reading that crossed the threshold is already a sample in its window, so
counting the alert as well would double `sample_count` and skew the mean. Every
telemetry event therefore has a read-side effect, and none of them appears in the
"not projected" list `tests/integration/test_projections.py` keeps.

### Rebuilding the telemetry read side

The rollups are derived state and can be rebuilt from the topic. The ledger is keyed by
`(consumer_group, event_id)` like every other projection's, so the rebuild is:

```sql
-- on the read instance
TRUNCATE "read_telemetry"."telemetry_rollups", "read_telemetry"."sensor_latest";
DELETE FROM "read_analytics"."processed_events"
WHERE consumer_group = 'plantkeeper-read-telemetry-rollup';
```

then restart the read side with the group's offsets reset (or delete the group and let
it start at `earliest`). Kafka's retention is the horizon: once `telemetry.events` has
discarded a reading, no replay can bring it back. `tests/integration/test_projections.py`
drives exactly this sequence and asserts the rollups come back identical.

## The silence timer

`SensorOffline` is in the catalogue, and until the audit remediation it could never
fire — nothing inspected silence and the sensor's last-seen instant was not persisted
with its readings. Both halves now exist:

* the ingress writes the sensor row with each stored reading, so `sensors.last_seen_at`
  is the newest instant the sensor reported and `sensors.offline_announced_at` records
  when the current silence was announced (`NULL` while it is reporting, cleared by the
  next reading);
* `SensorSilenceJob` (worker, `SENSOR_SILENCE_CHECK_INTERVAL_SECONDS`) polls the
  registry for sensors silent past `Sensor.OFFLINE_AFTER` (ten minutes) and asks each
  one to announce itself.

The aggregate decides, not the loop: `Sensor.mark_offline` refuses a sensor that is
still reporting, refuses one that never reported, and refuses one whose current silence
has already been announced. A sensor quiet for a week is therefore announced once, and
a sensor that reports and goes quiet again is a new silence with a new announcement.
The announcement commits in the same transaction as the sensor row that records it, so
a crash cannot leave a sensor looking unannounced while its `SensorOffline` is already
in the outbox. `NotificationConsumer` turns the event into a `sensor_offline` reminder
like any other telemetry fact.

## The ingress's transaction

One delivery is one transaction:

1. `INSERT … ON CONFLICT DO NOTHING` the reading;
2. call `Sensor.record(reading)` and append **every event the aggregate raised** to
   `write_shared.outbox` — `TelemetryReceived` always, plus the threshold event
   when the reading crossed one (`SoilMoistureLow`, `SoilMoistureHigh`,
   `TemperatureAnomaly`) — but only if the insert was the one that stored the row;
3. save the sensor row, which now carries the reading's instant as `last_seen_at` and
   has cleared `offline_announced_at`;
4. commit.

The sensor write is deliberately in that same transaction rather than in the timer:
without it the silence timer would have to scan the readings table to answer "which
sensors have gone quiet", and a sensor that had reported again would still look
announced. The aggregate is called for its state as well as its decisions.

A delivery whose row is already there therefore commits nothing and announces nothing:
its event travelled in the transaction that stored the reading, and a second
`TelemetryReceived` would carry a fresh `event_id` that no consumer-group ledger could
recognise as the duplicate it is. A crash before the commit loses nothing (Kafka
redelivers, and the table's key makes the retry a no-op); a crash after it re-inserts
nothing. The consumer group is `plantkeeper-telemetry-ingest`
(`TELEMETRY_INGEST_CONSUMER_GROUP`), and its offset reset is `latest` rather than the
sagas' `earliest`: raw telemetry has no ledger to rebuild against — the readings table
*is* the record — so a fresh group starts at the live edge instead of re-ingesting a log.

The ingress runs the platform's consumer failure policy like every other subscriber —
`CONSUMER_MAX_ATTEMPTS` attempts with backoff, then a copy to `plantkeeper.dlq.v1`
([ADR 0011](adr/0011-consumer-failure-policy.md)) — with one difference that follows
from having no ledger: its copy names no claim to clear, because there is none, and a
replay is absorbed by the reading's own key. An unparseable message, and one from a
sensor nobody registered, are dropped rather than retried: repeating either cannot
help, and the next reading lands as soon as the sensor is registered.

## Configuration

| Variable | Default | Meaning |
|----------|---------|---------|
| `TELEMETRY_RAW_TOPIC` | `telemetry.raw` | where the simulator publishes |
| `TELEMETRY_INGEST_CONSUMER_GROUP` | `plantkeeper-telemetry-ingest` | the ingress's consumer group |
| `TELEMETRY_PARTITION_MONTHS_AHEAD` | `3` | how far past the current month the window reaches |
| `TELEMETRY_PARTITION_CHECK_INTERVAL_SECONDS` | `86400` | how often the job looks |
| `TELEMETRY_RETENTION_MONTHS` | `12` | how long raw readings are kept; whole months past this are dropped |
| `SENSOR_SILENCE_CHECK_INTERVAL_SECONDS` | `60` | how often the silence timer looks for a sensor that has gone quiet |

There is no `TELEMETRY_BATCH_SIZE` knob: the ingress stores one delivery's reading per
transaction, because the transaction boundary is what makes the reading and its event
atomic. `SqlAlchemyTelemetryRepository.add_many` still chunks a batch it is handed (500
rows per statement, the bind-parameter ceiling), which a future batched reader can use
without changing the consumer's contract.
