# iot-telemetry Specification

## Purpose

The raw sensor stream, how each reading is stored and deduplicated, the events a reading raises, and the simulator that produces the stream.

## Requirements

### Requirement: Raw readings are not domain events

Sensor measurements SHALL arrive on a dedicated raw stream carrying only a sensor identifier, an instant and the measured values. A raw reading SHALL NOT be a domain event, SHALL NOT carry a plant, and SHALL have no meaning to any context until the write side resolves which plant the sensor belongs to. Domain events raised from readings SHALL travel on the context's own event topic instead, published by the relay like every other event.

#### Scenario: A reading is produced

- **WHEN** a measurement is published to the raw stream
- **THEN** it carries the sensor identifier, the instant and the measured values, and no plant identifier

#### Scenario: A reading is accepted

- **WHEN** the write side stores a reading
- **THEN** the domain event announcing it is published on the telemetry event topic by the relay, not by the producer of the raw reading

### Requirement: A reading must identify its instant and stay within physical ranges

A reading SHALL be accepted only when its sensor identifier is present, its instant carries a timezone, and its moisture, temperature and light values fall within the ranges the domain allows. A timestamp without a timezone SHALL be rejected, because the instant is part of the reading's identity. A message that does not validate SHALL be logged and acknowledged rather than retried, so that one malformed message cannot block the stream.

#### Scenario: A reading carries no timezone

- **WHEN** a reading arrives whose recorded instant has no timezone
- **THEN** the reading is rejected

#### Scenario: A measured value is out of range

- **WHEN** a reading reports a moisture, temperature or light value outside the range the domain allows
- **THEN** the reading is rejected

#### Scenario: A malformed message arrives

- **WHEN** a message on the raw stream cannot be read as a reading
- **THEN** it is logged with its identifier and acknowledged, and the stream continues with the next message

### Requirement: A reading is stored once, keyed by sensor and instant

Readings SHALL be stored append-only in a table keyed by the sensor identifier together with the recorded instant, and that key SHALL be the deduplication key. Storing the same reading again SHALL leave the table unchanged and SHALL announce nothing, so that a redelivery, a replayed capture or a second simulator run cannot duplicate history.

#### Scenario: The same reading is delivered twice

- **WHEN** a reading is delivered a second time
- **THEN** the table is unchanged and no event is announced

#### Scenario: A capture is replayed

- **WHEN** a recorded capture is replayed into the raw stream
- **THEN** readings already stored are ignored and only new instants are added

### Requirement: Readings are partitioned by month

The readings table SHALL be partitioned by the recorded month, SHALL keep partitions ahead of the current month, and SHALL hold a catch-all partition so that a reading always has somewhere to land even when its instant falls outside the created window.

#### Scenario: A reading arrives for a month with no partition

- **WHEN** a reading's recorded instant falls outside the partitions created so far
- **THEN** the reading is still stored, in the catch-all partition

#### Scenario: The partition job runs

- **WHEN** the partition job runs
- **THEN** the current month and the configured number of months ahead exist as their own partitions

### Requirement: A reading from an unregistered sensor is dropped

A reading whose sensor belongs to no plant SHALL be logged with a warning and dropped, and readings from that sensor SHALL land as soon as it is registered.

#### Scenario: An unregistered sensor reports

- **WHEN** a reading arrives from a sensor that is not registered
- **THEN** the reading is logged with a warning and dropped, and no event is announced

#### Scenario: The sensor is registered

- **WHEN** the sensor is registered and reports again
- **THEN** the reading is stored and announced

### Requirement: Storing a reading and announcing it commit together

Storing a reading and appending the events it raises SHALL happen in one transaction, so a crash cannot store a reading whose event was never announced or announce an event for a reading that was not stored.

#### Scenario: A crash happens before the commit

- **WHEN** the process stops before the transaction commits
- **THEN** the reading is not stored and no event is announced, and the redelivery stores and announces it once

### Requirement: A reading that crosses a threshold raises an event

A stored reading SHALL raise the moisture-low event, the moisture-high event or the temperature-anomaly event when it crosses the corresponding threshold, using the telemetry context's own thresholds so that no consumer has to restate them. Those events SHALL be consumed by the care and notification contexts.

#### Scenario: Moisture falls below the low threshold

- **WHEN** a stored reading reports moisture below the low threshold
- **THEN** a moisture-low event is announced

#### Scenario: Moisture rises above the high threshold

- **WHEN** a stored reading reports moisture above the high threshold
- **THEN** a moisture-high event is announced

#### Scenario: Temperature leaves the allowed band

- **WHEN** a stored reading reports a temperature outside the allowed band
- **THEN** a temperature-anomaly event is announced

#### Scenario: A reading sits exactly on a threshold

- **WHEN** a stored reading reports a value exactly at a threshold
- **THEN** no alert is raised, boundaries being inclusive

### Requirement: A silent sensor is not announced

The platform SHALL NOT publish an offline event, because no timer inspects how long a sensor has been silent. A sensor that stops reporting SHALL therefore produce no event and no notification.

#### Scenario: A sensor stops reporting

- **WHEN** a registered sensor stops sending readings for longer than the documented silence period
- **THEN** no offline event is published and no notification is created

### Requirement: Telemetry facts reach no read model

No read model SHALL be built from the telemetry event topic, so a reading is visible only in the readings table. This is a recorded decision rather than an oversight.

#### Scenario: A reading is stored

- **WHEN** a reading is stored and its events are published
- **THEN** no read model table contains it, and it is readable only from the readings table

### Requirement: The simulator produces a deterministic stream

The simulator SHALL model daylight, soil evaporation and sensor noise, and a run SHALL be reproducible from its seed together with its sensor count, scenario, sensor base, step length and start instant. Sensor identifiers SHALL be derived deterministically from a base identifier, and each sensor's noise SHALL be drawn from its own stream so that one sensor cannot shift another's readings. Evaporation SHALL be expressed as a half-life rather than a per-hour rate, so that the same parameters describe any step length.

#### Scenario: The same run is repeated

- **WHEN** the simulator runs twice with the same seed, parameters and start instant
- **THEN** it derives the same sensor identifiers and produces the same readings

#### Scenario: The start instant changes

- **WHEN** the same configuration runs from a different start instant
- **THEN** the sensor identifiers are unchanged and the readings differ, because daylight and temperature follow the clock

#### Scenario: A sensor is added to a run

- **WHEN** the sensor count changes
- **THEN** the existing sensors' readings are unchanged

### Requirement: The simulator offers named scenarios

The simulator SHALL offer the scenarios `normal`, `drought`, `overwatering`, `cold_snap` and `sensor_failure`, each changing the physical parameters so that a specific behaviour of the platform is exercised. Thresholds used by a scenario SHALL mirror the thresholds the domain raises its alerts on. Because the simulator may not import the domain, those values are duplicated rather than shared, so the two can drift apart undetected.

#### Scenario: The drought scenario runs

- **WHEN** the simulator runs the `drought` scenario
- **THEN** readings cross the low-moisture threshold immediately, which pulls a distant watering forward

#### Scenario: The overwatering scenario runs

- **WHEN** the simulator runs the `overwatering` scenario
- **THEN** readings stay above the high-moisture threshold

#### Scenario: The sensor failure scenario runs

- **WHEN** the simulator runs the `sensor_failure` scenario
- **THEN** the stream stops for each sensor in turn and then resumes

### Requirement: The simulator is runnable and observable from the command line

The simulator SHALL be startable as a module with a sensor count, an interval, a scenario, a seed, a sensor base, a broker address, a tick limit, a dry-run mode and a capture replay mode, each with a documented default. It SHALL report the sensor base identifier its sensor identifiers are derived from, so that an operator can derive and register the ones they intend to watch, SHALL write its logs to standard error so that a dry run's standard output stays a clean capture, and SHALL flush pending messages before exiting on an interrupt.

#### Scenario: The simulator starts

- **WHEN** the simulator starts
- **THEN** it reports the sensor base identifier, from which each sensor's own identifier is derived

#### Scenario: A dry run is captured

- **WHEN** the simulator runs in dry-run mode
- **THEN** its standard output is a stream of readings that can be replayed later, and its logs are on standard error

#### Scenario: A capture is replayed

- **WHEN** a capture produced by a dry run is replayed
- **THEN** the same readings are published to the broker

#### Scenario: The simulator is interrupted

- **WHEN** the simulator receives an interrupt
- **THEN** it flushes the readings it has queued before exiting

### Requirement: The simulator is independent of the application

The simulator SHALL NOT import the domain, application or infrastructure packages, and SHALL be a producer of raw measurements rather than of domain events.

#### Scenario: The simulator is built

- **WHEN** the project's import rules are checked
- **THEN** the simulator imports none of the application layers
