# Domain

> **Status:** Phase 0 skeleton. The glossary, aggregate map and invariants are filled in
> by Phase 1 — see [`ROADMAP.md`](../ROADMAP.md).

## Overview

The domain layer (`packages/domain`) is pure Python: it must not import infrastructure,
web frameworks or ORMs, and it is the only layer allowed to define business invariants.
This is enforced by the `import-linter` domain-purity contract (`make lint`).

## Ubiquitous language

| Term | Meaning |
|------|---------|
| Household | The single family unit that owns every plant in this installation |
| Plant | A physical plant in the household, bound to a species and a location |
| Species | A catalogue entry describing ideal care (from Trefle) |
| Care schedule | The next watering/fertilising/repotting dates for one plant |
| Sensor reading | One measurement: moisture, temperature, light level, timestamp |
| Journal entry | An immutable record of care performed |
| Notification | A message for the household, delivered by HTTP long polling |

*(Table is completed in Phase 1.)*

## Bounded contexts and aggregates

| Context | Aggregates | Invariants |
|---------|-----------|------------|
| Garden | `Plant`, `Household` | Name is not empty; no two waterings within an hour; no repotting more often than every 6 months; at most 50 plants per household |
| Care | `CareSchedule` | Interval > 0; next watering not in the past; `version` increments on every change |
| Catalog | `Species` | Required Trefle fields present; versioned |
| Journal | `JournalEntry`, `JournalStream` | Append-only, immutable |
| Telemetry | `Sensor` | One sensor belongs to exactly one plant; `sensor_id` is unique |
| Notifications | `Notification` | Delivered at most once; `read_at` is set by an explicit ack |
| Identity | `User` | A member belongs to exactly one household (no authentication) |

## Value objects

`HouseholdId`, `PlantId`, `SensorId`, `SpeciesId`, `JournalEntryId`, `Location`,
`Moisture`, `Temperature`, `LightLevel`, `WateringInterval`, `CareRule`, `SensorReading`.

All value objects are immutable and validate their own boundaries on construction.

## Domain events

Garden: `PlantAdded`, `PlantRemoved`, `PlantMoved`, `PlantOnboarded`.
Care: `CareScheduleCreated`, `WateringDue`, `WateringCompleted`, `WateringRescheduled`,
`CareMissed`, `CareSkipped`.
Catalog: `SpeciesSyncRequested`, `SpeciesUpdated`, `SpeciesCacheInvalidated`.
Journal: `JournalEntryAdded`.
Telemetry: `TelemetryReceived`, `SoilMoistureLow`, `SoilMoistureHigh`,
`TemperatureAnomaly`, `SensorOffline`.
Notifications: `NotificationCreated`, `NotificationRead`.
Saga/system: `SagaStarted`, `SagaCompleted`, `SagaFailed`, `SagaCompensated`.

The per-event payload catalogue (`producer`, `consumer`, JSON schema) lives in
`docs/events.md`, added in the phase that defines the events.

## References

- [`docs/architecture.md`](architecture.md) — contexts, layers, data flow
- [`ROADMAP.md`](../ROADMAP.md) — Phase 1 builds this domain
