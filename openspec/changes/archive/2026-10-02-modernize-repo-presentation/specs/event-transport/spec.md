# Spec Delta

## Purpose

How a fact becomes a message other contexts can rely on: the topic layout, the message contract, the outbox that makes publication atomic, and the idempotency every consumer owes.

## ADDED Requirements

### Requirement: One topic per bounded context

Each bounded context SHALL publish its events to a topic of its own rather than one topic per event type, and a consumer SHALL subscribe once to a topic and select the events it handles by name.

#### Scenario: A context publishes several kinds of event

- **WHEN** a context publishes events of different types
- **THEN** they all travel on that context's topic

#### Scenario: A consumer cares about some of a context's events

- **WHEN** a consumer subscribes to a context's topic
- **THEN** it receives every event of that context and handles only the ones it owns

### Requirement: The message body is the event and the rest travels in headers

A published message SHALL carry the event document as its body, with the event's name and identifier in headers so that a consumer can deserialise without guessing the type and can deduplicate. A message that was moved aside as unprocessable SHALL also carry the topic it came from and the error.

#### Scenario: A consumer reads a message

- **WHEN** a consumer receives a message
- **THEN** it knows the event's name and identifier from the headers, before examining the body

#### Scenario: A message is moved aside

- **WHEN** a message is abandoned after repeated failures
- **THEN** it carries the topic it came from and the error that caused the abandonment

### Requirement: Events are immutable documents that reject unknown fields

Every event SHALL be an immutable document that refuses unknown fields, and its payload SHALL be built from domain values and identifiers rather than arbitrary structures. A deliberate exception to a value-typed payload SHALL be documented where it occurs.

#### Scenario: An event document carries an unexpected field

- **WHEN** an event body containing a field the event does not define is deserialised
- **THEN** deserialisation fails rather than ignoring the field

#### Scenario: A value object reaches the wire

- **WHEN** an event carrying a value object is published
- **THEN** the value it wraps appears in the body, and the event still validates when read back

### Requirement: Messages are keyed by the aggregate they concern

A message SHALL be keyed by the first aggregate identifier its payload carries, falling back to the event's own identifier, so that all events of one aggregate land in one partition and stay in order. A producer SHALL NOT hold global ordering, and ordering SHALL NOT be promised across different aggregates or contexts.

#### Scenario: Several events concern one plant

- **WHEN** several events for the same plant are published
- **THEN** they share a partition key and are consumed in the order they were produced

#### Scenario: Events concern different aggregates

- **WHEN** events for different plants are published
- **THEN** they may be consumed in any relative order

### Requirement: A fact and its publication are committed atomically

Every write use case SHALL commit the aggregate's new state and the events it raised in one transaction, and publication SHALL be performed afterwards by the relay. A request handler SHALL NOT publish to the broker directly, and no write path SHALL bypass the outbox.

#### Scenario: A command succeeds

- **WHEN** a command completes
- **THEN** the aggregate's change and its events are committed together, and the broker has not yet been contacted

#### Scenario: The relay publishes

- **WHEN** the relay runs
- **THEN** it reads unpublished rows and publishes them, and no other component produces domain events

### Requirement: Delivery is at least once

Publication SHALL be at-least-once: a failure between the broker accepting a message and the row being marked published SHALL cause the message to be published again on a later poll. Consumers SHALL therefore tolerate a repeated delivery.

#### Scenario: The process dies after the broker accepted a message

- **WHEN** the relay stops after the broker accepted a message but before the row was marked published
- **THEN** the message is published again on the next poll

### Requirement: The relay retries, then abandons a message without dropping it

The relay SHALL retry a transient broker failure within a poll, SHALL track how many polls a message has failed, and after the configured threshold SHALL copy the message to the abandoned-message topic and mark the row accordingly. If that copy itself fails, the row SHALL be retried rather than discarded, so that no message is dropped silently.

#### Scenario: The broker is briefly unavailable

- **WHEN** publishing fails transiently within a poll
- **THEN** the relay retries the publish before counting the poll as failed

#### Scenario: A message keeps failing

- **WHEN** a message has failed the configured number of polls
- **THEN** it is copied to the abandoned-message topic and its row is marked

#### Scenario: The abandoned-message copy fails

- **WHEN** copying a message to the abandoned-message topic fails
- **THEN** the message is retried instead of being discarded

### Requirement: Every consumer is idempotent on consumer group and event id

Every consumer SHALL claim each delivery in a ledger keyed by its consumer group together with the event identifier, and SHALL claim it in the same transaction as the work it performs, so that a rolled-back failure releases the claim and a redelivery starts from nothing.

#### Scenario: A delivery is processed

- **WHEN** a consumer handles a delivery
- **THEN** the claim and its work commit together

#### Scenario: A delivery is repeated

- **WHEN** a consumer receives a delivery it has already claimed
- **THEN** it does nothing and acknowledges

### Requirement: The telemetry ingress is the one consumer without a ledger

A consumer that ingests raw sensor readings SHALL NOT keep a delivery ledger: it SHALL be idempotent on the reading key of sensor and recorded instant instead, because a raw reading has no event identifier until the write side creates one and because the readings table is itself the record.

#### Scenario: A raw reading is redelivered

- **WHEN** the same raw reading is delivered twice
- **THEN** the second delivery stores nothing and announces nothing, without consulting a ledger

#### Scenario: A raw topic is replayed

- **WHEN** the ingress is restarted against a replayed raw topic
- **THEN** no duplicate reading and no duplicate event results

### Requirement: A consumer uses its own group and never blocks its partition

Every consumer SHALL consume under a consumer group of its own. On an event whose name it does not know, or a body that does not validate, it SHALL log the delivery and acknowledge it rather than retrying or halting the partition.

#### Scenario: An unknown event arrives

- **WHEN** a consumer receives an event name it does not recognise
- **THEN** it logs the delivery and acknowledges it

#### Scenario: A body fails validation

- **WHEN** a consumer receives a body that does not validate
- **THEN** it logs the delivery with its identifier and acknowledges it

### Requirement: A domain rule violation is answered rather than retried, where the platform decides

A write request that breaks a domain rule SHALL be answered with a status describing the failure rather than retried, and only infrastructure failures SHALL be retried. A consumer does not yet make that distinction: an error raised while handling a delivery propagates and the broker redelivers it. That is a recorded gap rather than intended behaviour, and no consumer claims terminal handling it does not have.

#### Scenario: A request breaks a domain rule

- **WHEN** a command or query breaks a domain invariant
- **THEN** the caller receives a status describing the failure and the request is not retried

#### Scenario: A delivery breaks a domain rule

- **WHEN** handling a delivery raises
- **THEN** the error propagates and the delivery is redelivered, and this gap is recorded rather than presented as terminal handling

### Requirement: Contexts communicate only by events

No bounded context SHALL import another context's internals, call another context synchronously, or read another context's tables. A context SHALL learn another's facts from that context's events.

#### Scenario: A context needs another context's data

- **WHEN** a context needs a fact another context owns
- **THEN** it consumes the event carrying that fact

#### Scenario: The cross-context rule is broken

- **WHEN** one context imports another's internals
- **THEN** the project's import rules fail the build

### Requirement: The event catalogue is published with the contracts

The set of events and the topics they travel on SHALL be available as machine-readable data attached to the generated contract documents, so that the catalogue cannot drift from the code.

#### Scenario: The contracts are generated

- **WHEN** the contract documents are regenerated
- **THEN** each carries the event catalogue derived from the code
