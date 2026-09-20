# Domain events

> **Status:** Phase 5 — the catalogue below is exactly what `packages/domain`
> implements (`tests/unit/domain/test_events_catalogue.py` guards it), its
> transport is implemented in
> [`packages/infrastructure/.../messaging/topics.py`](../packages/infrastructure/src/plantkeeper/infrastructure/messaging/topics.py),
> and its consumers are the read side's projections
> ([`docs/cqrs.md`](cqrs.md)), the write side's sagas
> ([`docs/sagas.md`](sagas.md)) and the telemetry ingress
> ([`docs/telemetry.md`](telemetry.md)). Every event now has a producer. The
> AsyncAPI document is Phase 10.

## Event contract

- Every event is a frozen Pydantic v2 model with `extra="forbid"`, subclassing
  `plantkeeper.domain.base.DomainEvent`.
- Inherited fields: `event_id: UUID` (UUIDv7 by default, monotonic within a
  process) and `occurred_at: AwareDatetime` (timezone-aware, UTC in practice).
- Serialisation is `model_dump_json()`; deserialisation is `model_validate_json()`.
- Payload fields are domain value objects and identifiers, never dataclasses or raw
  `dict`s (except `NotificationCreated.payload`, which is deliberately `JsonValue`).
- A value object reaches the wire as the value it wraps: `Location` is `"Shelf"`,
  `WateringInterval` is `"P7D"` (ISO-8601 duration), an identifier is its UUID
  string. The wrapper exists so the domain can tell `Moisture` from `Temperature`;
  it is not part of the integration contract. Validation still accepts the wrapped
  `{"value": ...}` shape, so an older row or a hand-written message loads.
- Aggregates record events with `_record(...)`; the application layer drains them
  with `collect_events()` and writes them to the transactional outbox (Phase 2).
- Names are past tense: `<Subject><Verb>`.

## Transport

An event is published by the outbox relay, never by a request handler, and the
Kafka message is deliberately envelope-less: the body **is** the event document,
and everything a consumer needs beyond it travels in the headers.

**Topics — one per bounded context, not one per event type.** A consumer that
cares about three Garden events subscribes once and dispatches on the
`event_name` header. Twenty-five topics would push the broker's metadata cost onto
every consumer for no benefit at this scale.

| Topic | Events |
|-------|--------|
| `garden.events` | `PlantAdded`, `PlantRemoved`, `PlantMoved`, `PlantOnboarded` |
| `care.events` | `CareScheduleCreated`, `WateringDue`, `WateringCompleted`, `WateringRescheduled`, `CareMissed`, `CareSkipped` |
| `catalog.events` | `SpeciesSyncRequested`, `SpeciesUpdated`, `SpeciesCacheInvalidated` |
| `journal.events` | `JournalEntryAdded` |
| `telemetry.events` | `TelemetryReceived`, `SoilMoistureLow`, `SoilMoistureHigh`, `TemperatureAnomaly`, `SensorOffline` |
| `notifications.events` | `NotificationCreated`, `NotificationRead` |
| `saga.events` | `SagaStarted`, `SagaCompleted`, `SagaFailed`, `SagaCompensated` |
| `telemetry.raw` | **raw sensor JSON, not a domain event** — see below |
| `plantkeeper.dlq.v1` | messages the relay gave up on, from any of the above |

**`telemetry.raw` is the one exception to "every message on Kafka is a domain event".**
It carries what a sensor reported — `sensor_id`, `recorded_at`, `moisture`,
`temperature`, `light` — published directly by `tools/iot-simulator` rather than by the
relay. A raw measurement has no `plant_id` and no business meaning until the write side's
telemetry ingress resolves one, stores the reading and appends the `TelemetryReceived`
that *is* a domain event to its transactional outbox. The envelope, the validation
policy and the table are in [`docs/telemetry.md`](telemetry.md).

**Body** — `json.dumps(event.model_dump(mode="json"), separators=(",", ":"))`,
compact so the payload is the event and nothing else.

**Headers** — `event_name` and `event_id`, so a consumer can deserialise without
guessing the type and can deduplicate on `(consumer_group, event_id)`. A
dead-lettered message also carries `original_topic` and `error`, which is enough
for an operator to replay it to the right place without decoding the body.

**Key** — the first payload field present among `plant_id`, `household_id`,
`species_id`, `sensor_id`, `saga_id`, else `event_id` (`partition_key_for`). This
keeps every event of one aggregate in one partition, and therefore in order,
without the producer holding global ordering. The saga lifecycle events carry no
aggregate id, so `saga_id` is what keeps a saga's four messages together.

**Delivery is at-least-once.** A crash between Kafka accepting a message and the
outbox row being marked published republishes it on the next poll, so every
consumer must be idempotent on `(consumer_group, event_id)`. That is not an
implementation detail of the relay: it is the contract each consumer signs. The
read side keeps it in `read_analytics.processed_events`
([`docs/cqrs.md`](cqrs.md)); the write side's sagas keep theirs in
`write_shared.processed_events` ([`docs/sagas.md`](sagas.md)).

**Consumers.** A consumer subscribes to a topic with a consumer group of its own
and dispatches on the `event_name` header — a topic carries every event of its
context, so most deliveries are somebody else's. The read side runs one group per
projection (`<READ_SIDE_CONSUMER_GROUP_PREFIX>-garden`, `…-care`, `…-catalog`,
`…-notifications`, `…-journal`) with `auto_offset_reset="earliest"`, which is what
makes a rebuilt read table possible. The write side runs one group per consumer
(`<WORKER_CONSUMER_GROUP_PREFIX>-onboard-plant`, `…-adaptive-watering`,
`…-missed-care`, `…-species-sync`, `…-journal-entries`). A consumer that meets an unknown
`event_name`, or a body that does not validate, logs it with the `event_id` and
acknowledges it rather than blocking the partition behind a contract violation.

**Failure handling.** `attempts` counts failed polls, not individual produce
calls; `tenacity` retries a transient broker error three times inside one poll.
After `OUTBOX_MAX_ATTEMPTS` (5) failed polls the message is copied to the
dead-letter topic and the row is marked `dead_lettered_at`. If the copy itself
fails the row is retried instead, so no message is dropped silently. See
[ADR 0003](adr/0003-write-side-outbox.md).

## Catalogue

The consumers column names what exists today and what is still planned. The
projections are the read side's ([`docs/cqrs.md`](cqrs.md)); the sagas are the
write side's ([`docs/sagas.md`](sagas.md)).

### Garden

| Event | Payload | Emitted by | Consumed by |
|-------|---------|-----------|-------------|
| `PlantAdded` | `plant_id`, `household_id`, `species_id`, `name`, `location`, `added_at` | `Plant` (`add`) | `GardenProjection`; `OnboardPlantSaga` |
| `PlantRemoved` | `plant_id`, `removed_at` | `Plant` (`remove`) | `GardenProjection` |
| `PlantMoved` | `plant_id`, `previous_location`, `location` | `Plant` (`move`) | `GardenProjection` |
| `PlantOnboarded` | `plant_id`, `household_id`, `species_id`, `next_watering_at` | `OnboardPlantSaga` | `GardenProjection` |

### Care

| Event | Payload | Emitted by | Consumed by |
|-------|---------|-----------|-------------|
| `CareScheduleCreated` | `plant_id`, `watering_interval`, `next_watering_at` | `CareSchedule` (`create`) | `CareProjection` |
| `WateringDue` | `plant_id`, `due_at` | `CareSchedule` (`mark_due`), `MissedCareSaga`'s scheduler | `MissedCareSaga`; `NotificationConsumer` |
| `WateringCompleted` | `plant_id`, `completed_at`, `next_watering_at` | `CareSchedule` (`complete_watering`) | `CareProjection`; `MissedCareSaga`; `JournalEntryConsumer` |
| `WateringRescheduled` | `plant_id`, `previous_next_watering_at`, `next_watering_at`, `reason` | `CareSchedule` (`reschedule`), `AdaptiveWateringSaga` | `CareProjection`; `NotificationConsumer` |
| `CareMissed` | `plant_id`, `next_watering_at` | `CareSchedule` (`mark_missed`), `MissedCareSaga` | `CareProjection`; `NotificationConsumer` |
| `CareSkipped` | `plant_id`, `skipped_at`, `next_watering_at` | `CareSchedule` (`skip`) | `CareProjection` |

### Catalog

| Event | Payload | Emitted by | Consumed by |
|-------|---------|-----------|-------------|
| `SpeciesSyncRequested` | *(no payload)* | `SpeciesSyncScheduler`, `POST /api/v1/catalog/sync` | `SpeciesSyncSaga` |
| `SpeciesUpdated` | `species_id`, `scientific_name`, `common_name`, `watering_interval`, `light_requirement`, `version` | `Species` (`update`), `SpeciesSyncSaga`'s compensation | `SpeciesProjection`; Valkey cache (Phase 9) |
| `SpeciesCacheInvalidated` | `species_id` | `SpeciesSyncSaga` | Valkey species cache (Phase 9) |

### Journal

| Event | Payload | Emitted by | Consumed by |
|-------|---------|-----------|-------------|
| `JournalEntryAdded` | `entry_id`, `plant_id`, `entry_type`, `note`, `entry_occurred_at` | `JournalEntry` (`add`) | `JournalProjection`; Analytics |

`entry_occurred_at` is when the care happened; the inherited `occurred_at` is when
the entry was recorded.

`JournalEntryAdded` is the fact of an **event-sourced** aggregate: the journal stores
it in `write_journal.event_store` and derives its state by replaying the stream
(`docs/event-sourcing.md`). `JournalEntryConsumer` is what produces one for a
watering: it consumes `WateringCompleted` and appends a `WATERING` entry whose care
moment is the event's `completed_at`.

### Telemetry

| Event | Payload | Emitted by | Consumed by |
|-------|---------|-----------|-------------|
| `TelemetryReceived` | `sensor_id`, `plant_id`, `recorded_at`, `moisture`, `temperature`, `light` | the telemetry ingress (from `telemetry.raw`, through `Sensor.record`) | Care (`AdaptiveWateringSaga`); the readings table stores the same fact |
| `SoilMoistureLow` | `sensor_id`, `plant_id`, `moisture`, `threshold` | `Sensor` (`record`), called by the telemetry ingress | `NotificationConsumer` |
| `SoilMoistureHigh` | `sensor_id`, `plant_id`, `moisture`, `threshold` | `Sensor` (`record`), called by the telemetry ingress | `AdaptiveWateringSaga` |
| `TemperatureAnomaly` | `sensor_id`, `plant_id`, `temperature`, `low_threshold`, `high_threshold` | `Sensor` (`record`), called by the telemetry ingress | `NotificationConsumer` |
| `SensorOffline` | `sensor_id`, `plant_id`, `last_seen_at`, `offline_for` | `Sensor` (`mark_offline`) | Notifications (no producer yet) |

`TelemetryReceived` is the one catalogued event with no aggregate behind it: a reading is
an append-only fact, so nothing raises it for the aggregate to own — the ingress appends
it to the outbox explicitly, in the same transaction as the reading. The three threshold
events *are* raised by the aggregate: since Phase 8 the ingress calls
`Sensor.record(reading)`, drains the events it raised and appends each of them to the
same outbox rowset. The sensor's own state change (`last_seen_at`) is not persisted —
that stays Phase 5's decision. `SensorOffline` still has no caller: it needs a silence
timer, which no phase has built.

### Notifications

| Event | Payload | Emitted by | Consumed by |
|-------|---------|-----------|-------------|
| `NotificationCreated` | `notification_id`, `household_id`, `notification_type`, `payload`, `created_at` | `Notification` (`create`), the notification producers | `NotificationProjection`; `NotificationPusher` (the Valkey nudge behind HTTP long polling) |
| `NotificationRead` | `notification_id`, `household_id`, `read_at` | `Notification` (`mark_read`) | `NotificationProjection` |

### Saga / system

| Event | Payload | Emitted by | Consumed by |
|-------|---------|-----------|-------------|
| `SagaStarted` | `saga_id`, `saga_name` | `Saga.handle_event` | operators; any saga observer |
| `SagaCompleted` | `saga_id`, `saga_name` | `Saga.handle_event` | operators; any saga observer |
| `SagaFailed` | `saga_id`, `saga_name`, `error` | `Saga.handle_event` | operators; any saga observer |
| `SagaCompensated` | `saga_id`, `saga_name` | `Saga.handle_event` | operators; any saga observer |

These four report the progress of the process managers rather than a state change
in a bounded context; they live in the catalogue because every message on Kafka is
a `DomainEvent`. Nothing projects them — the saga's execution row in
`write_shared.saga_state` is the authoritative record, and these are its event
stream. See [`docs/sagas.md`](sagas.md).

## Deliberate non-events

These domain facts intentionally produce **no** event, so that the catalogue stays
small and every event has a real cross-context consumer:

- Creating a `Household`, a `User` or a `Species`: the write side persists them, and
  no other context reacts.
- Registering a `Sensor`: the sensor-to-plant mapping is the write side's own
  bookkeeping. `.docs/plan.md` UF-5 mentions a `SensorAdded` event; it is not in this
  catalogue and will only be added if a Phase 2 consumer needs it.
- `Plant.water` and `Plant.repot`: they advance the Garden invariants' timestamps,
  but the watering fact is Care's `WateringCompleted` and the repotting fact is
  Journal's `JournalEntryAdded`.

## Deferred to later phases

- Windowed aggregation over the readings (a moving moisture average) and a telemetry
  read model — not scheduled; `docs/telemetry.md` describes what is here.
- AsyncAPI document — Phase 10.

## Consumers

The read side's projections listen to the topics above with one Kafka consumer
group per read model, each idempotent through `read_analytics.processed_events`.
What each one consumes and writes, how to run it, and how to rebuild a read model
from its topic are in [`docs/cqrs.md`](cqrs.md); the decisions are in
[ADR 0004](adr/0004-read-side-projections.md).

The write side's sagas listen with one group per saga and claim their deliveries in
`write_shared.processed_events`. What each one reacts to, its steps, its
compensations and how to inspect and recover it are in
[`docs/sagas.md`](sagas.md); the decisions are in
[ADR 0005](adr/0005-orchestration-vs-choreography.md).

Two more write-side consumers joined in Phase 8 and follow the same discipline:
`NotificationConsumer` (group `…-notifications`) turns care and telemetry facts into
notifications, and `NotificationPusher` (group `…-notification-push`) turns
`NotificationCreated` into the Valkey nudge a long poll waits on
([`docs/notifications.md`](notifications.md)).

The telemetry ingress is a consumer of a different kind: it reads `telemetry.raw`, not a
topic from the table above, and its consumer group is
`plantkeeper-telemetry-ingest`. It keeps no `(consumer_group, event_id)` ledger, because
the readings table's `(sensor_id, recorded_at)` key already provides exactly that
idempotency. [`docs/telemetry.md`](telemetry.md) has the detail.
