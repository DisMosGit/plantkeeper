# Spec Delta

## MODIFIED Requirements

### Requirement: The message body is the event and the rest travels in headers

A published message SHALL carry the event document as its body, with the event's name and identifier in headers so that a consumer can deserialise without guessing the type and can deduplicate. Every message SHALL also carry the identifier of the message that caused it and the identifier of the conversation it belongs to, the name of the actor or component that raised it, the version of the event's schema and the trace context, so that a chain of reactions can be followed across topics and an event's origin is never anonymous. A message that was moved aside as unprocessable SHALL also carry the topic it came from and the error.

#### Scenario: A consumer reads a message

- **WHEN** a consumer receives a message
- **THEN** it knows the event's name and identifier from the headers, before examining the body

#### Scenario: A chain of reactions is followed

- **WHEN** an event causes a process manager to raise another event
- **THEN** the second message carries the first message's identifier as its cause and shares its conversation identifier, so the whole chain is reconstructable

#### Scenario: An event's origin is questioned

- **WHEN** an operator asks who or what raised an event
- **THEN** the message names the actor, scheduler, process or service that raised it

#### Scenario: A message is moved aside

- **WHEN** a message is abandoned after repeated failures
- **THEN** it carries the topic it came from and the error that caused the abandonment

### Requirement: Messages are keyed by the aggregate they concern

A message SHALL be keyed by the first aggregate identifier its payload carries, falling back to the event's own identifier, so that all events of one aggregate land in one partition and stay in order. Publication SHALL preserve the order of the outbox within a key: a message that has not yet been published SHALL hold back the later messages sharing its key until it is published or abandoned. A producer SHALL NOT hold global ordering, and ordering SHALL NOT be promised across different aggregates or contexts.

#### Scenario: Several events concern one plant

- **WHEN** several events for the same plant are published
- **THEN** they share a partition key and are consumed in the order they were produced

#### Scenario: A message of one key keeps failing

- **WHEN** a message cannot be published while later messages share its key
- **THEN** those later messages wait, and a consumer never sees the later event before the earlier one

#### Scenario: Events concern different aggregates

- **WHEN** events for different plants are published
- **THEN** they may be consumed in any relative order

### Requirement: The relay retries, then abandons a message without dropping it

The relay SHALL claim the rows it works on, so that more than one relay may run at the same time without publishing a row twice or reordering a key. It SHALL retry a transient broker failure within a poll, SHALL track how many polls a message has failed, and after the configured threshold SHALL copy the message to the abandoned-message topic and mark the row accordingly. If that copy itself fails, the row SHALL be retried rather than discarded, so that no message is dropped silently.

#### Scenario: Two relays run at once

- **WHEN** more than one relay polls the outbox
- **THEN** each row is published by exactly one of them and no key is published out of order

#### Scenario: The broker is briefly unavailable

- **WHEN** publishing fails transiently within a poll
- **THEN** the relay retries the publish before counting the poll as failed

#### Scenario: A message keeps failing

- **WHEN** a message has failed the configured number of polls
- **THEN** it is copied to the abandoned-message topic and its row is marked

#### Scenario: The abandoned-message copy fails

- **WHEN** copying a message to the abandoned-message topic fails
- **THEN** the message is retried instead of being discarded

### Requirement: A domain rule violation is answered rather than retried, where the platform decides

A write request that breaks a domain rule SHALL be answered with a status describing the failure rather than retried, and only infrastructure failures SHALL be retried. A consumer SHALL make the same distinction: a failure it can retry SHALL be retried a bounded number of times with backoff, and a delivery that keeps failing SHALL be moved aside with its claim recorded and its cause named, so that one poison delivery cannot hold a partition forever and cannot be handled twice.

#### Scenario: A request breaks a domain rule

- **WHEN** a command or query breaks a domain invariant
- **THEN** the caller receives a status describing the failure and the request is not retried

#### Scenario: A delivery breaks a domain rule

- **WHEN** handling a delivery breaks a domain invariant and cannot succeed
- **THEN** the delivery is not retried forever; it is moved aside with its claim and its cause recorded

#### Scenario: A delivery fails transiently

- **WHEN** handling a delivery fails in a way that may succeed later
- **THEN** it is retried a bounded number of times with backoff before being given up on

#### Scenario: A delivery cannot be handled

- **WHEN** a delivery keeps failing past the retry budget
- **THEN** it is moved aside to the abandoned-message topic with its claim and its cause recorded, and the consumer moves on

### Requirement: Contexts communicate only by events

No bounded context SHALL import another context's internals, call another context synchronously, or read another context's tables. A context SHALL learn another's facts from that context's events, and a context that needs a durable reference to another context's aggregate SHALL keep that reference in a table of its own, filled from the events that context publishes.

#### Scenario: A context needs another context's data

- **WHEN** a context needs a fact another context owns
- **THEN** it consumes the event carrying that fact

#### Scenario: A context needs a stable reference to another's aggregate

- **WHEN** a consumer of care or journal facts needs to know which plant a fact concerns
- **THEN** it reads its own reference rows, which were maintained from the garden context's events, rather than the garden context's tables

#### Scenario: The cross-context rule is broken

- **WHEN** one context imports another's internals
- **THEN** the project's import rules fail the build
