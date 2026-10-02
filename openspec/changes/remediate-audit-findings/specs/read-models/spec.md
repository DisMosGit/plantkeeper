# Spec Delta

## MODIFIED Requirements

### Requirement: The read side and the write side never read each other's tables

The read side SHALL keep its own schema, managed by its own migrations, on a database instance of its own, and SHALL NOT read any write-side table. The write side SHALL NOT read any read-side table. A write-side read SHALL serve only the needs of a command — validation, or the answer the caller is owed right after its own write — and SHALL NOT stand in for a query about what the system looks like.

#### Scenario: A read model is queried

- **WHEN** a read model is served to a client
- **THEN** the answer is assembled only from read-side tables, on the read side's own database

#### Scenario: A command is handled

- **WHEN** a command changes the domain
- **THEN** it reads and writes only write-side tables

#### Scenario: The read side is unavailable

- **WHEN** the read side's database cannot be reached
- **THEN** commands still succeed on the write side, and only the query side is degraded

### Requirement: Unprojected events are recorded as decisions

The events the read side deliberately does not project SHALL be named in the test suite, so that a gap is a decision rather than an oversight. That list SHALL include the schedule-became-due event and the catalogue's sync trigger and cache invalidation. Telemetry events SHALL NOT be on that list, because they are projected into the telemetry read side.

#### Scenario: An event is left unprojected

- **WHEN** a context publishes an event that no projection handles
- **THEN** the event is named in the test that lists unprojected events, with the reason it is not projected

#### Scenario: A telemetry fact is published

- **WHEN** a telemetry event is published
- **THEN** the telemetry read side projects it into its rollups, and the list of unprojected events does not name it

## ADDED Requirements

### Requirement: Client queries that ask what the system looks like are answered from read models

A query whose question is what the system looks like — a list, a timeline for browsing, a report — SHALL be answered from read models. The answer to the caller's own command, and the reads a command needs to enforce its rules, SHALL be answered from the write side so that a successful command is never invisible to its caller. A client SHALL tolerate the window in which a read model has not yet caught up.

#### Scenario: A list is requested

- **WHEN** a client requests a list of plants, schedules, notifications or journal entries
- **THEN** the answer is assembled from the read models and reflects projected state

#### Scenario: A command has just succeeded

- **WHEN** a client receives the answer to its own command
- **THEN** the answer reflects the command's own result, taken from the write side

#### Scenario: The read model lags

- **WHEN** a list is requested moments after a command changed the same rows
- **THEN** the list may not yet show the change and is served regardless
