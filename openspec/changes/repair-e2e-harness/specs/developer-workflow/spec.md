# Spec Delta

## ADDED Requirements

### Requirement: The worker whose components the suite runs is startable

Because the suite runs the worker's own components rather than stand-ins, the worker's
entry point SHALL resolve every dependency its job list is built from, at the scope it
asks for it. A binding a background job needs outside a request SHALL be obtainable
without shadowing — or being shadowed by — the same type's request-scoped binding, and
a unit test SHALL assert that resolution instead of running the process.

#### Scenario: The entry point asks for a dependency of its job list

- **WHEN** the worker's entry point resolves what `build_background_jobs` runs on
- **THEN** the container answers, and the same type stays request-bound for the consumers that need that binding

#### Scenario: A dependency is provided in two scopes

- **WHEN** one type is needed both for the process and for a single request
- **THEN** each caller gets the binding its scope requires, rather than one binding replacing the other

### Requirement: The end-to-end suite runs the process it asserts against

A flow that crosses processes SHALL be verified against the components the workers
target actually runs for that flow, started through a single shared definition rather
than a copy per test module, so that a component added to the runnable worker is one
edit for the suite instead of a component that can be forgotten by eight of them. When
a flow's completion depends on a background job rather than on a message handler, that
job SHALL be running while the test waits for the flow to finish. An interval-driven
timer SHALL NOT be started implicitly by the suite: a test whose flow depends on one
SHALL drive it explicitly, so that a scheduled tick cannot change another test's facts.

#### Scenario: A flow is completed by a background job

- **WHEN** an end-to-end test covers a flow whose completion a background job performs
- **THEN** the shared harness has that job running and the test observes the completed flow

#### Scenario: The runnable worker gains a component

- **WHEN** a component is added to the workers target
- **THEN** adding it to the end-to-end suite is a single edit to the shared harness

#### Scenario: A flow depends on an interval-driven timer

- **WHEN** an end-to-end test's flow depends on a scheduled tick
- **THEN** that test drives the timer itself rather than relying on the suite to run it

### Requirement: An end-to-end test observes only its own events

An end-to-end test SHALL observe only the events it caused. A consumer group started
for a test SHALL NOT be offered events another test published, and no test SHALL depend
on, or be disturbed by, facts a previous test left on the broker. The isolation SHALL be
established once by the suite rather than opted into by each test, and SHALL cover every
topic the platform publishes events on, so that a topic added to the event catalogue is
isolated without editing a hand-maintained list. The suite SHALL therefore produce the
same result when it is run repeatedly on the same commit.

#### Scenario: Events from an earlier test are still on the broker

- **WHEN** a test starts its consumers after another test has already published
- **THEN** it is offered only the events that test publishes

#### Scenario: A replayed fact concerns rows that no longer exist

- **WHEN** an event published by an earlier test would concern state the current test has cleared
- **THEN** it is not delivered to the current test's consumers and changes nothing the test asserts on

#### Scenario: The event catalogue gains a topic

- **WHEN** a topic is added to the platform's event catalogue
- **THEN** the suite isolates it without a hand-maintained topic list

#### Scenario: The suite is run twice

- **WHEN** the end-to-end suite is run twice in succession on the same commit
- **THEN** the same tests pass in both runs
