# Spec Delta

## Purpose

The query side: read models shaped for the questions that are actually asked, kept in step with the write side's events and rebuildable from a topic.

## ADDED Requirements

### Requirement: The read side and the write side never read each other's tables

The read side SHALL keep its own schema, managed by its own migrations, and SHALL NOT read any write-side table. The write side SHALL NOT read any read-side table.

#### Scenario: A read model is queried

- **WHEN** a read model is served to a client
- **THEN** the answer is assembled only from read-side tables

#### Scenario: A command is handled

- **WHEN** a command changes the domain
- **THEN** it reads and writes only write-side tables

### Requirement: Read models are shaped by the questions asked, not copied from the write schema

A read model SHALL be denormalised for the query it serves, so that a page renders in one read rather than by joining across the schema.

#### Scenario: The plant list is rendered

- **WHEN** the plant list is read
- **THEN** each plant's species name and next watering moment are already on the row, without a further lookup

### Requirement: One consumer group per projection, dispatching on the event name

Each projection SHALL consume its topic under a consumer group of its own, and SHALL dispatch on the event's name, ignoring events belonging to other consumers of the same topic.

#### Scenario: A topic carries an event the projection does not handle

- **WHEN** a projection receives an event it does not own
- **THEN** it acknowledges the delivery without writing anything

#### Scenario: A projection restarts

- **WHEN** a projection starts
- **THEN** it consumes from the beginning of its topic, which is what makes a read model rebuildable

### Requirement: A projection claims a delivery in the same transaction as its writes

A projection SHALL record the delivery in its own ledger, keyed by consumer group and event id, in the same transaction as the read-model writes. A failure SHALL roll back both, so a redelivery starts from nothing rather than from a ledger row claiming the work was done.

#### Scenario: A projection fails part way

- **WHEN** a projection fails while writing a read model
- **THEN** both the ledger row and the read-model writes are rolled back, and the redelivery applies the event once

#### Scenario: The same event is delivered twice

- **WHEN** the same event is delivered to the same group again
- **THEN** the ledger already holds the claim and nothing is written

### Requirement: One writer per read-model column

A read model assembled from several topics SHALL have each column written by exactly one projection, and a projection SHALL leave the columns it does not own untouched. A row whose other columns have not arrived yet SHALL be allowed to exist and SHALL read as incomplete rather than as wrong.

#### Scenario: A care event arrives before the plant that owns it

- **WHEN** a projection receives an event for a plant whose row has not been created yet
- **THEN** the row is created with only that projection's columns filled and the remaining columns read as empty

#### Scenario: A later event fills another column

- **WHEN** the projection that owns another column later projects its event
- **THEN** it fills its own columns and leaves the existing ones unchanged

### Requirement: A read model can be rebuilt from its topic

Discarding a read model and replaying its topic SHALL reproduce it. The documented procedure SHALL be to stop the read side, remove the group's tables and its ledger rows, delete the group's offsets, and restart it. Kafka's retention SHALL be documented as the horizon of any rebuild.

#### Scenario: A read model is rebuilt

- **WHEN** a group's tables, ledger rows and offsets are removed and the read side is restarted
- **THEN** the group replays its topic from the start and the read model is reproduced

#### Scenario: The topic no longer holds an event

- **WHEN** a rebuild runs after the topic has discarded events the read model needs
- **THEN** the rebuilt model is incomplete, and the limitation is documented rather than silent

### Requirement: Unprojected events are recorded as decisions

The events the read side deliberately does not project SHALL be named in the test suite, so that a gap is a decision rather than an oversight. That list SHALL include the schedule-became-due event, the catalogue's sync trigger and cache invalidation, and every telemetry event.

#### Scenario: An event is left unprojected

- **WHEN** a context publishes an event that no projection handles
- **THEN** the event is named in the test that lists unprojected events, with the reason it is not projected

#### Scenario: A telemetry fact is published

- **WHEN** a telemetry event is published
- **THEN** no projection consumes it and the decision is recorded

### Requirement: The read side is read-only for its operators

The administrative view over the read models SHALL offer no operation that changes a read model. Locally it SHALL render as a single superuser selected automatically, with no login form, and that behaviour SHALL be switchable off.

#### Scenario: An operator opens the administrative view

- **WHEN** an operator browses a read model in the administrative view
- **THEN** no control for creating, changing or deleting a row is offered

#### Scenario: Automatic local access is disabled

- **WHEN** the automatic local user is unset
- **THEN** the administrative view falls back to its own authentication

### Requirement: The read side is eventually consistent with the write side

A fact SHALL be visible on the write side before it is visible in a read model, and clients SHALL tolerate that window. No read SHALL block waiting for a projection to catch up.

#### Scenario: A command has just committed

- **WHEN** a client reads a read model immediately after a command succeeds
- **THEN** the read model may not yet reflect the command, and the read is served regardless

### Requirement: The delivery ledger is inspectable

The ledger that records which deliveries each projection has claimed SHALL be inspectable per consumer group, so that a projection that is behind can be identified.

#### Scenario: A projection falls behind

- **WHEN** an operator inspects the read side's ledger
- **THEN** the deliveries claimed by a named consumer group are visible
