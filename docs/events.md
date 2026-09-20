# Domain events

> **Status:** Phase 2 — the catalogue below is exactly what `packages/domain`
> implements (`tests/unit/domain/test_events_catalogue.py` guards it), and its
> transport is implemented in
> [`packages/infrastructure/.../messaging/topics.py`](../packages/infrastructure/src/plantkeeper/infrastructure/messaging/topics.py).
> The AsyncAPI document is Phase 10; the consumers column is the plan from
> [`docs/architecture.md`](architecture.md).

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
`event_name` header. Twenty-one topics would push the broker's metadata cost onto
every consumer for no benefit at this scale.

| Topic | Events |
|-------|--------|
| `garden.events` | `PlantAdded`, `PlantRemoved`, `PlantMoved`, `PlantOnboarded` |
| `care.events` | `CareScheduleCreated`, `WateringDue`, `WateringCompleted`, `WateringRescheduled`, `CareMissed`, `CareSkipped` |
| `catalog.events` | `SpeciesSyncRequested`, `SpeciesUpdated`, `SpeciesCacheInvalidated` |
| `journal.events` | `JournalEntryAdded` |
| `telemetry.events` | `TelemetryReceived`, `SoilMoistureLow`, `SoilMoistureHigh`, `TemperatureAnomaly`, `SensorOffline` |
| `notifications.events` | `NotificationCreated`, `NotificationRead` |
| `plantkeeper.dlq.v1` | messages the relay gave up on, from any of the above |

**Body** — `json.dumps(event.model_dump(mode="json"), separators=(",", ":"))`,
compact so the payload is the event and nothing else.

**Headers** — `event_name` and `event_id`, so a consumer can deserialise without
guessing the type and can deduplicate on `(consumer_group, event_id)`. A
dead-lettered message also carries `original_topic` and `error`, which is enough
for an operator to replay it to the right place without decoding the body.

**Key** — the first payload field present among `plant_id`, `household_id`,
`species_id`, `sensor_id`, else `event_id` (`partition_key_for`). This keeps every
event of one aggregate in one partition, and therefore in order, without the
producer holding global ordering.

**Delivery is at-least-once.** A crash between Kafka accepting a message and the
outbox row being marked published republishes it on the next poll, so every
consumer must be idempotent on `(consumer_group, event_id)`. That is not an
implementation detail of the relay: it is the contract each consumer signs.

**Failure handling.** `attempts` counts failed polls, not individual produce
calls; `tenacity` retries a transient broker error three times inside one poll.
After `OUTBOX_MAX_ATTEMPTS` (5) failed polls the message is copied to the
dead-letter topic and the row is marked `dead_lettered_at`. If the copy itself
fails the row is retried instead, so no message is dropped silently. See
[ADR 0003](adr/0003-write-side-outbox.md).

## Catalogue

### Garden

| Event | Payload | Emitted by | Consumed by (planned) |
|-------|---------|-----------|-----------------------|
| `PlantAdded` | `plant_id`, `household_id`, `species_id`, `name`, `location`, `added_at` | `Plant` (`add`) | Care (OnboardPlantSaga), Analytics |
| `PlantRemoved` | `plant_id`, `removed_at` | `Plant` (`remove`) | Care, Analytics |
| `PlantMoved` | `plant_id`, `previous_location`, `location` | `Plant` (`move`) | Analytics |
| `PlantOnboarded` | `plant_id`, `household_id`, `species_id`, `next_watering_at` | OnboardPlantSaga (Phase 4) | Notifications, Analytics |

### Care

| Event | Payload | Emitted by | Consumed by (planned) |
|-------|---------|-----------|-----------------------|
| `CareScheduleCreated` | `plant_id`, `watering_interval`, `next_watering_at` | `CareSchedule` (`create`) | Analytics |
| `WateringDue` | `plant_id`, `due_at` | `CareSchedule` (`mark_due`) | Notifications, MissedCareSaga (Phase 4) |
| `WateringCompleted` | `plant_id`, `completed_at`, `next_watering_at` | `CareSchedule` (`complete_watering`) | Journal, Analytics |
| `WateringRescheduled` | `plant_id`, `previous_next_watering_at`, `next_watering_at`, `reason` | `CareSchedule` (`reschedule`), AdaptiveWateringSaga (Phase 4) | Notifications, Analytics |
| `CareMissed` | `plant_id`, `next_watering_at` | `CareSchedule` (`mark_missed`), MissedCareSaga (Phase 4) | Notifications, Analytics |
| `CareSkipped` | `plant_id`, `skipped_at`, `next_watering_at` | `CareSchedule` (`skip`) | Analytics |

### Catalog

| Event | Payload | Emitted by | Consumed by (planned) |
|-------|---------|-----------|-----------------------|
| `SpeciesSyncRequested` | *(no payload)* | Daily scheduler, `POST /api/v1/catalog/sync` (Phase 2) | SpeciesSyncSaga |
| `SpeciesUpdated` | `species_id`, `scientific_name`, `common_name`, `watering_interval`, `light_requirement`, `version` | `Species` (`update`) | Garden read models, cache invalidation |
| `SpeciesCacheInvalidated` | `species_id` | SpeciesSyncSaga (Phase 9) | Valkey species cache |

### Journal

| Event | Payload | Emitted by | Consumed by (planned) |
|-------|---------|-----------|-----------------------|
| `JournalEntryAdded` | `entry_id`, `plant_id`, `entry_type`, `note`, `entry_occurred_at` | `JournalEntry` (`add`) | Journal projection, Analytics |

`entry_occurred_at` is when the care happened; the inherited `occurred_at` is when
the entry was recorded.

### Telemetry

| Event | Payload | Emitted by | Consumed by (planned) |
|-------|---------|-----------|-----------------------|
| `TelemetryReceived` | `sensor_id`, `plant_id`, `recorded_at`, `moisture`, `temperature`, `light` | `Sensor` (`record`) | Care (AdaptiveWateringSaga), telemetry projection |
| `SoilMoistureLow` | `sensor_id`, `plant_id`, `moisture`, `threshold` | `Sensor` (`record`) | Care, Notifications |
| `SoilMoistureHigh` | `sensor_id`, `plant_id`, `moisture`, `threshold` | `Sensor` (`record`) | Care, Notifications |
| `TemperatureAnomaly` | `sensor_id`, `plant_id`, `temperature`, `low_threshold`, `high_threshold` | `Sensor` (`record`) | Notifications, Care |
| `SensorOffline` | `sensor_id`, `plant_id`, `last_seen_at`, `offline_for` | `Sensor` (`mark_offline`) | Notifications |

### Notifications

| Event | Payload | Emitted by | Consumed by (planned) |
|-------|---------|-----------|-----------------------|
| `NotificationCreated` | `notification_id`, `household_id`, `notification_type`, `payload`, `created_at` | `Notification` (`create`) | HTTP long polling (Phase 8), Analytics |
| `NotificationRead` | `notification_id`, `household_id`, `read_at` | `Notification` (`mark_read`) | Analytics |

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

- `SagaStarted`, `SagaCompleted`, `SagaFailed`, `SagaCompensated` — Phase 4.
- AsyncAPI document — Phase 10.
