# Spec Delta

## MODIFIED Requirements

### Requirement: Each step commits its own transaction and a failure compensates

Each step of an orchestrated process SHALL commit in its own transaction, and that one transaction SHALL carry the step's effect together with the step-history entry recording it, the process's state checkpoint and any lifecycle event describing that progress — all of them or none. When a step fails, the steps already completed SHALL be compensated in fresh transactions, and the process SHALL record its failure and its compensation in the same transaction as the delivery that ran it.

#### Scenario: A step commits

- **WHEN** a step of an orchestrated process completes
- **THEN** its effect, its step-history entry, its state checkpoint and its lifecycle event commit together

#### Scenario: The process stops inside a step

- **WHEN** the process stops after a step began but before its transaction committed
- **THEN** the step's effect and its recorded progress are both absent, and the resumed process runs the step again

#### Scenario: A middle step fails

- **WHEN** a step fails after earlier steps have committed
- **THEN** the earlier steps are compensated and the process records its failure and compensation

#### Scenario: A step fails before anything committed

- **WHEN** the first step fails
- **THEN** no compensation is needed and the process records its failure

### Requirement: A process manager writes the way a command does

A step that changes a context the process owns SHALL commit that change and its events to the outbox in one unit of work, the way a command does. A step that changes a context the process does not own SHALL record a command for that context in the same transaction as the process's own progress, and that command SHALL be executed afterwards by a consumer through the owning context's own handlers and unit of work, at most once per recorded command. A process manager SHALL NOT call another context synchronously, SHALL NOT call another context through its API, and SHALL NOT write another context's tables.

#### Scenario: A step creates a schedule

- **WHEN** an onboarding step creates a care schedule
- **THEN** the command is recorded in the same transaction as the process's progress, the care context later executes it through its own unit of work, and the resulting event is committed to the outbox and published by the relay

#### Scenario: The process stops after recording a command

- **WHEN** the process stops between recording a command and that command being executed
- **THEN** the resumed process or the dispatcher executes the command exactly once and no second effect appears

#### Scenario: A step needs another context's data

- **WHEN** a step needs a fact another context owns
- **THEN** it reads through its own port or reacts to a published event, never through another context's API

### Requirement: A process manager's own lifecycle is observable and ordered

Starting, completing, failing and compensating SHALL each publish an event, and each lifecycle event SHALL be committed in the same transaction as the state change it reports, so that the published lifecycle and the recorded state cannot disagree. Those events SHALL be keyed so that one process's messages stay in a single partition and in order. No read model SHALL project them, and the recorded process state SHALL be the authoritative view of its progress.

#### Scenario: A process completes

- **WHEN** a process finishes
- **THEN** its completion is recorded and its lifecycle events are published in order on the process-manager topic

#### Scenario: The lifecycle and the state are compared

- **WHEN** an operator compares a process's lifecycle events with its recorded state
- **THEN** the two agree, because each event was committed with the state change it reports

#### Scenario: Process progress is queried

- **WHEN** an operator wants a process's progress
- **THEN** the recorded state is authoritative rather than the lifecycle events

## ADDED Requirements

### Requirement: An interrupted process is resumed and a recorded failure is retried within a budget

A process left running or compensating without progress SHALL be resumed from its recorded step history, skipping the steps already taken and finishing an interrupted compensation. A process whose failure was recorded SHALL be retried within a bounded budget of attempts, and only once that budget is exhausted SHALL it be parked for an operator. A parked process SHALL be resettable by an operator so that it runs again from its recorded history.

#### Scenario: A process is interrupted without recording a failure

- **WHEN** a process stops between steps and is later found stale
- **THEN** it is resumed from its step history and completes

#### Scenario: A recorded failure is retried

- **WHEN** a process has recorded a failure and its retry budget is not yet exhausted
- **THEN** it is re-run from its recorded step history

#### Scenario: The retry budget is exhausted

- **WHEN** a process has been retried the permitted number of times and still fails
- **THEN** it is parked and neither redelivery nor recovery re-runs it

#### Scenario: An operator resets a parked process

- **WHEN** an operator resets a parked process
- **THEN** it is resumed from its recorded step history and may run again

## REMOVED Requirements

### Requirement: An interrupted process is resumed, a recorded failure is not

**Reason**: Making a recorded failure final turns a transient fault — a broker outage, a deadlock, an upstream timeout — into permanent data loss for that process, and leaves no tooling to correct it.

**Migration**: The replacement requirement retries a recorded failure within a bounded budget and then parks the process for an operator, who can reset it. The recovery job's bounded attempt counter is reused as the budget.
