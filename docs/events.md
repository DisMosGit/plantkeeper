# Domain events

> **Status:** Phase 1 — the catalogue below is exactly what `packages/domain`
> implements (`tests/unit/domain/test_events_catalogue.py` guards it). Kafka topics
> and the AsyncAPI document are assigned in Phase 2 and Phase 10; the consumers
> column is the plan from [`docs/architecture.md`](architecture.md).

## Event contract

- Every event is a frozen Pydantic v2 model with `extra="forbid"`, subclassing
  `plantkeeper.domain.base.DomainEvent`.
- Inherited fields: `event_id: UUID` (UUIDv7 by default, monotonic within a
  process) and `occurred_at: AwareDatetime` (timezone-aware, UTC in practice).
- Serialisation is `model_dump_json()`; deserialisation is `model_validate_json()`.
- Payload fields are domain value objects and identifiers, never dataclasses or raw
  `dict`s (except `NotificationCreated.payload`, which is deliberately `JsonValue`).
- Aggregates record events with `_record(...)`; the application layer drains them
  with `collect_events()` and writes them to the transactional outbox (Phase 2).
- Names are past tense: `<Subject><Verb>`.

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
| `SpeciesSyncRequested` | *(no payload)* | Daily scheduler, `POST /api/v1/catalog/sync` (Phase 4) | SpeciesSyncSaga |
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
- Kafka topic names, AsyncAPI and message keys — Phase 2/10.
