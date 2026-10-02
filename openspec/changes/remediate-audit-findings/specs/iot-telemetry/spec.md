# Spec Delta

## ADDED Requirements

### Requirement: A silent sensor is announced at most once per silence

A sensor's last seen instant SHALL be persisted with its readings, and a timer SHALL publish an offline event when a registered sensor has been silent for longer than the documented silence period. The event SHALL carry the sensor, the plant it watches, the instant it was last seen and how long it has been silent, and SHALL be published at most once per silence, so that a sensor quiet for a week is announced once rather than every tick. A consumer MAY turn that event into a notification the same way it turns the threshold events into ones.

#### Scenario: A sensor stops reporting

- **WHEN** a registered sensor has been silent for longer than the silence period
- **THEN** one offline event is published for that silence

#### Scenario: The timer runs again during the same silence

- **WHEN** the timer runs again and the sensor is still silent after its offline event was published
- **THEN** no second offline event is published

#### Scenario: The sensor reports again and then goes quiet

- **WHEN** a sensor reports after an offline event and then falls silent past the period again
- **THEN** a new offline event is published for the new silence

### Requirement: Telemetry facts reach a dedicated read side of rollups

Telemetry events SHALL be projected, under a consumer group of its own, into a read schema of their own that keeps a fixed-window rollup per sensor — at least the minimum, average and maximum moisture and the sample count — and a latest row per sensor. The raw readings table SHALL remain the detail record and SHALL NOT be copied into the domain read models. The rollups SHALL be rebuildable by replaying the telemetry event topic.

#### Scenario: Readings arrive

- **WHEN** telemetry events arrive for a sensor
- **THEN** the rollup covering their window is updated and the sensor's latest row reflects the newest reading

#### Scenario: A window is revisited

- **WHEN** an out-of-order reading lands in a window that already has a rollup
- **THEN** the rollup is recomputed for that window rather than duplicated

#### Scenario: The telemetry read side is rebuilt

- **WHEN** the rollups, their ledger rows and the group's offsets are removed and the read side is restarted
- **THEN** the rollups are reproduced by replaying the telemetry event topic

#### Scenario: The domain read models are inspected

- **WHEN** the domain read models are inspected
- **THEN** no raw reading has been copied into them

### Requirement: Readings have a retention window

Raw readings SHALL be kept only for a documented retention window, enforced by dropping whole month partitions that have aged past it, while the rollups outlive the raw rows they were computed from.

#### Scenario: A month partition ages out

- **WHEN** a readings partition lies entirely beyond the retention window
- **THEN** it is dropped and the rollups covering those months remain

#### Scenario: A reading is older than the window

- **WHEN** a reading arrives whose instant lies in a partition that has already been dropped
- **THEN** it lands in the catch-all partition, and the limit is documented rather than silent

## REMOVED Requirements

### Requirement: A silent sensor is not announced

**Reason**: The catalogued offline event could never fire because no timer inspected silence and the sensor's last-seen instant was never persisted, so the catalogue promised a fact the platform could not produce. The silence timer now exists.

**Migration**: The replacement requirement announces a silence at most once per silence. Consumers that today react only to the threshold events MAY also react to the offline event; the notification consumer gains a producer for it in the same change.

### Requirement: Telemetry facts reach no read model

**Reason**: Telemetry is the platform's highest-volume stream and its only query path was the write-side readings table, leaving the event topic with no consumer on the read side and the platform with no way to show what its sensors measured. The gap was recorded rather than intended.

**Migration**: Telemetry events are projected into a dedicated rollup read side (see the replacement requirement), kept out of the domain read models so their shape and rebuild story are unaffected.
