# care-scheduling Specification

## Purpose

When each plant should be watered next, how that plan changes when care happens or is skipped, and how a dry reading pulls the plan forward.

## Requirements

### Requirement: A plant has exactly one watering schedule

Each plant SHALL have at most one watering schedule, identified by the plant it belongs to, and creating a schedule SHALL record the interval and the next watering moment.

#### Scenario: A schedule is created for a plant

- **WHEN** a schedule is created for a plant with a watering interval
- **THEN** the schedule exists under that plant's identifier, and a schedule-created event carries the interval and the next watering moment

### Requirement: A schedule never plans the past and never has a non-positive interval

A watering interval SHALL be strictly greater than zero, and the next watering moment SHALL never be scheduled before the current moment.

#### Scenario: A past moment is scheduled

- **WHEN** a change would set the next watering moment before now
- **THEN** the change is refused

#### Scenario: A zero interval is supplied

- **WHEN** a schedule is created with a zero or negative interval
- **THEN** the value is refused

### Requirement: Every schedule change is versioned

Every change to a schedule SHALL increment its version, and a change that supplies a version other than the current one SHALL be refused rather than applied.

#### Scenario: Two writers change the same schedule

- **WHEN** two changes are submitted against the same schedule version
- **THEN** the first applies and the second is refused as a conflict

#### Scenario: A change supplies the current version

- **WHEN** a change supplies the version the schedule currently holds
- **THEN** the change applies and the version increases by one

### Requirement: Care state changes are published as events

Completing a watering, rescheduling, skipping and missing SHALL each publish their own event, and a schedule falling due SHALL publish a due event.

#### Scenario: A watering is completed

- **WHEN** a watering is completed
- **THEN** a watering-completed event carries the moment it happened and the next watering moment

#### Scenario: A schedule falls due

- **WHEN** a schedule's next watering moment arrives
- **THEN** a watering-due event is published once for that arrival

#### Scenario: Care is skipped

- **WHEN** a household skips the care of a plant
- **THEN** a care-skipped event is published and the schedule moves to its next moment

### Requirement: A dry reading pulls a watering forward only when it is far enough away

When a reading shows moisture below the low threshold, the next watering SHALL be moved to the present moment only if it was more than two days away. The threshold SHALL be the telemetry context's own constant, so that telemetry and its consumer cannot disagree about what "dry" means.

#### Scenario: Dry soil and a distant watering

- **WHEN** a reading below the low threshold arrives for a plant whose next watering is five days away
- **THEN** the next watering moves to the present moment and a rescheduled event is published

#### Scenario: Dry soil and an imminent watering

- **WHEN** a reading below the low threshold arrives for a plant whose next watering is later today
- **THEN** the schedule is left alone

#### Scenario: A stream of dry readings

- **WHEN** a plant whose watering has just been pulled forward keeps reporting dry soil every ten seconds
- **THEN** the condition no longer holds and no further reschedule is published

### Requirement: A missed watering shifts the schedule and is recorded once

A schedule that stays unwatered past its grace period SHALL be marked missed, which shifts the schedule and publishes a missed event, and an arrival after the deadline SHALL NOT erase the miss.

#### Scenario: The grace period passes without a watering

- **WHEN** a schedule has been due for longer than its grace period
- **THEN** it is marked missed, a care-missed event is published, and the schedule moves to its next moment

#### Scenario: A watering arrives after the deadline

- **WHEN** a watering is completed after the grace period has already expired
- **THEN** the miss stands and is not undone

#### Scenario: The check runs repeatedly

- **WHEN** the due check runs every minute while a schedule is already being watched
- **THEN** the due event is published once, not once per check
