# Spec Delta

## Purpose

How a household is told that something happened, and how a waiting client receives a message without polling the database in a loop.

## ADDED Requirements

### Requirement: A notification is created with the fact that caused it

Creating a notification SHALL happen in the same transaction as the delivery that caused it — or, when a process manager creates one as a step, in that step's own transaction — and SHALL publish a notification-created event from that transaction. A household's notifications SHALL be readable in the order they were created.

#### Scenario: A fact produces a notification

- **WHEN** a fact that requires a notification is consumed
- **THEN** the notification row and its event are committed together and the notification belongs to the household

#### Scenario: The same delivery is redelivered

- **WHEN** the delivery that created a notification is delivered again
- **THEN** no second notification is created

### Requirement: Each notification type has exactly one producer

Every notification type SHALL be produced by exactly one place, so that two writers cannot disagree about what a household is told.

#### Scenario: A notification type is produced

- **WHEN** a notification of any type is created
- **THEN** it was created by the single producer responsible for that type

### Requirement: Notifications are delivered by long polling

A client SHALL be able to request a household's unread notifications and wait for one to appear. The request SHALL answer immediately when there is something to deliver, SHALL answer with an empty result once the wait has elapsed with nothing to deliver, and SHALL reject a wait length outside the supported bounds.

#### Scenario: Something is already waiting

- **WHEN** a client requests pending notifications and unread ones exist
- **THEN** they are returned without waiting

#### Scenario: Nothing appears during the wait

- **WHEN** a client waits for the permitted maximum and nothing is created
- **THEN** the request answers with an empty result rather than hanging

#### Scenario: An unsupported wait is requested

- **WHEN** a client requests a wait length outside the supported bounds
- **THEN** the request is rejected

### Requirement: The wake-up signal is payload-free and delivery always reads the database

Waking a waiting client SHALL use a signal that carries no content and is scoped to the household. The endpoint SHALL always answer from the application's own tables, never from the signal, so that a lost signal costs at most one wait of latency and a duplicated or spurious signal costs at most a redundant read. Nothing SHALL be delivered exclusively through the signal, and the signal being unavailable SHALL degrade latency rather than lose a notification.

#### Scenario: A signal is lost

- **WHEN** a wake-up signal is never received
- **THEN** the waiting request still finds the notification when it re-reads before answering

#### Scenario: The signal channel is unavailable

- **WHEN** the signal cannot be published while notifications are still being created
- **THEN** notifications are not lost, and waiting clients find them on their next request

#### Scenario: The signal carries no content

- **WHEN** a client is woken by the signal
- **THEN** it learns only that something changed for its household and reads the content from the application

### Requirement: Acknowledgement is explicit and happens at most once

A notification SHALL be marked read only by an explicit acknowledgement, and a second acknowledgement of the same notification SHALL be refused as a conflict. Acknowledging SHALL publish a read event.

#### Scenario: A notification is acknowledged

- **WHEN** a client acknowledges a notification
- **THEN** it is marked read and no longer returned as pending

#### Scenario: A notification is acknowledged twice

- **WHEN** a client acknowledges a notification that is already read
- **THEN** the second acknowledgement is refused as a conflict

### Requirement: A rebuilt notification consumer creates nothing new

A notification that a consumer creates SHALL derive its identity from the fact that caused it, so that a consumer rebuilt from the start of a topic recognises notifications it already created and creates nothing further, even if its own delivery ledger has been lost. A notification created by a process-manager step SHALL instead be guarded by that process's recorded state, which is what stops a redelivery creating a second one.

#### Scenario: A consumer group is rebuilt

- **WHEN** a notification consumer's offsets are reset and the topic is replayed
- **THEN** no duplicate notification is created for a fact already recorded

### Requirement: Repeated sensor readings do not produce repeated reminders

A notification caused by a sensor reading SHALL be created at most once per plant while an equivalent notification is still unread, so that a sensor reporting the same condition all afternoon produces one message rather than one per reading.

#### Scenario: A sensor reports the same condition repeatedly

- **WHEN** readings keep crossing the same threshold while the resulting notification is unread
- **THEN** no further notification is created for that plant

#### Scenario: The notification is read and the condition persists

- **WHEN** the notification has been acknowledged and the condition is still reported
- **THEN** a new notification may be created

### Requirement: Notification delivery does not depend on an external channel

Notifications SHALL be delivered only to clients that are polling the API, and the platform SHALL NOT send them through electronic mail, a chat service or a mobile push provider.

#### Scenario: A household has no client connected

- **WHEN** a notification is created while nobody is waiting for that household
- **THEN** it is stored and delivered when a client next asks

### Requirement: The wake-up is scoped to a household rather than a notification type

The wake-up signal SHALL notify every waiting client of a household when anything changes for that household, and SHALL NOT be narrowed per notification type.

#### Scenario: Two clients wait on one household

- **WHEN** one notification is created for a household with two waiting clients
- **THEN** both are woken, and each reads the notification from the application
