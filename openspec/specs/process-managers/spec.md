# process-managers Specification

## Purpose

The long-running reactions that span several transactions, several aggregates or a wall-clock delay, and how they are recovered when they are interrupted.

## Requirements

### Requirement: A process manager is either orchestrated or choreographed

A process manager that owns an ordered sequence of steps SHALL keep that sequence and the compensation for each step explicitly. A process manager that owns nothing but its reaction SHALL be an ordinary consumer with no step list.

#### Scenario: An orchestrated process runs

- **WHEN** an orchestrated process manager handles its trigger
- **THEN** its steps run in the declared order and each step's compensation is available if a later step fails

#### Scenario: A choreographed process runs

- **WHEN** a choreographed process manager receives an event it reacts to
- **THEN** it makes its decision and writes the result, holding no step list

### Requirement: An orchestrated process is identified deterministically and records its progress

An orchestrated process manager SHALL derive its identifier from its trigger, so that a redelivery resumes the same instance rather than starting a second one. It SHALL record its execution state and its step history so that progress survives a restart.

#### Scenario: A trigger is redelivered

- **WHEN** the event that starts an orchestrated process is delivered again
- **THEN** the same instance is found and no second process is started

#### Scenario: A process is inspected

- **WHEN** an operator inspects a running process
- **THEN** its status and the steps it has already taken are readable

### Requirement: Each step commits its own transaction and a failure compensates

Each step of an orchestrated process SHALL commit in its own transaction. When a step fails, the steps already completed SHALL be compensated in fresh transactions, and the process SHALL end in a failed state with its failure and compensation published.

#### Scenario: A middle step fails

- **WHEN** a step fails after earlier steps have committed
- **THEN** the earlier steps are compensated and the process ends failed

#### Scenario: A step fails before anything committed

- **WHEN** the first step fails
- **THEN** no compensation is needed and the process ends failed

### Requirement: A published event cannot be recalled, so publishing is ordered last

A step that publishes an event SHALL be ordered so that compensation never has to unpublished it. Compensation SHALL NOT claim to undo an event that has already been published, and the resulting staleness on the read side SHALL be documented.

#### Scenario: Compensation runs after an event was published

- **WHEN** a process is compensated after one of its steps published an event
- **THEN** the event stands, and the read side may keep a row until the read model is rebuilt

### Requirement: Onboarding a plant resolves its species, schedules care and announces the result

Onboarding SHALL be triggered by a plant being added, SHALL resolve the plant's species from the local catalogue, SHALL create the plant's care schedule and its first notification, and SHALL announce that the plant is onboarded.

#### Scenario: A plant is added

- **WHEN** a plant is added to a household
- **THEN** its species is resolved, a care schedule is created for it, a notification is created for the household, and an onboarded event is published

#### Scenario: Onboarding fails part way

- **WHEN** a step of onboarding fails after the schedule was created
- **THEN** the schedule is removed and the process ends failed

### Requirement: Adaptive watering reacts to readings and pulls a distant watering forward

The adaptive watering process SHALL react to readings and, when soil is dry and the next watering is far enough away, SHALL move that watering to the present. It SHALL create an overwatering reminder at most once while an equivalent reminder is unread.

#### Scenario: Dry soil with a distant watering

- **WHEN** a reading below the low threshold arrives and the next watering is beyond the process's horizon
- **THEN** the watering is moved to the present and the change is published

#### Scenario: Overwatering is reported repeatedly

- **WHEN** readings keep above the high threshold while the reminder is unread
- **THEN** one reminder exists for that plant

### Requirement: Missed care is tracked over a grace period

The missed-care process SHALL open a grace window when a schedule falls due, SHALL consider the window satisfied when the watering happens inside it, and SHALL mark the schedule missed once the deadline passes. Its periodic check SHALL be idempotent, and a watering arriving after the deadline SHALL NOT erase the miss.

#### Scenario: A schedule falls due

- **WHEN** a schedule becomes due
- **THEN** a grace window is opened for it with a deadline

#### Scenario: The plant is watered inside the window

- **WHEN** the watering is completed before the deadline
- **THEN** the window is satisfied and no miss is recorded

#### Scenario: The deadline passes

- **WHEN** the deadline passes with the window still pending
- **THEN** the schedule is marked missed, the miss is published, and the schedule moves to its next moment

#### Scenario: The periodic check runs repeatedly

- **WHEN** the check runs again while a window is already open
- **THEN** no second window is opened and no second due event is published

### Requirement: A catalogue synchronisation runs as an orchestrated process with compensation

Synchronising the catalogue SHALL run as an orchestrated process whose steps fetch, apply and then invalidate, so that the step that can fail after committing is the one compensation exists for. Applying SHALL remember what it created and changed so that compensation can undo it.

#### Scenario: Applying fails after committing

- **WHEN** the apply step fails after committing some changes
- **THEN** the created entries are deleted and the changed ones are restored

### Requirement: An interrupted process is resumed, a recorded failure is not

A process left running or compensating without progress SHALL be resumed from its recorded step history, skipping the steps already taken and finishing an interrupted compensation, up to a bounded number of attempts. A process whose failure was recorded SHALL NOT be resumed automatically and SHALL be left for an operator.

#### Scenario: A process is interrupted without recording a failure

- **WHEN** a process stops between steps and is later found stale
- **THEN** it is resumed from its step history and completes

#### Scenario: Recovery keeps failing

- **WHEN** a process fails to recover more than the permitted number of times
- **THEN** it is left alone for an operator

#### Scenario: A failure was recorded

- **WHEN** a process has already recorded its failure
- **THEN** neither a redelivery nor recovery re-runs it

### Requirement: A process manager's own lifecycle is observable and ordered

Starting, completing, failing and compensating SHALL each publish an event, and those events SHALL be keyed so that one process's messages stay in a single partition and in order. No read model SHALL project them, and the recorded process state SHALL be the authoritative view of its progress.

#### Scenario: A process completes

- **WHEN** a process finishes
- **THEN** its lifecycle events are published in order on the process-manager topic

#### Scenario: Process progress is queried

- **WHEN** an operator wants a process's progress
- **THEN** the recorded state is authoritative rather than the lifecycle events

### Requirement: A process manager writes the way a command does

A step that changes the domain SHALL go through the same unit of work as a command and commit its events to the outbox in that transaction. A process manager SHALL NOT call another context through its API, and SHALL NOT write another context's tables.

#### Scenario: A step creates a schedule

- **WHEN** an onboarding step creates a care schedule
- **THEN** the schedule and its event are committed through the shared unit of work and the relay publishes the event

#### Scenario: A step needs another context's data

- **WHEN** a step needs a fact another context owns
- **THEN** it reads through its own port or reacts to a published event, never through another context's API
