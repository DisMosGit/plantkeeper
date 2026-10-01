# Spec Delta

## Purpose

The external contracts the platform offers — REST, gRPC and its message schemas — and the tests that fail when any of them drifts from the code they describe.

## ADDED Requirements

### Requirement: The REST contract is generated from the code

The REST contract SHALL be produced by importing the application without starting a server, and the document the running API serves SHALL be that same generated document. It SHALL NOT be written by hand.

#### Scenario: The contract is regenerated

- **WHEN** the contract generation command runs
- **THEN** the REST document is rebuilt from the application's own declarations

#### Scenario: The API serves its documentation

- **WHEN** the running API is asked for its documentation
- **THEN** it serves the generated document for the code that is running

### Requirement: The message contracts are generated from the real registrations

The message documents SHALL be built from the same broker registrations the write-side workers and the read side actually use, and each document SHALL carry the full event catalogue, so that neither document reports an event as consumed by nobody when the other side consumes it.

#### Scenario: The message documents are generated

- **WHEN** the message contracts are regenerated
- **THEN** each carries the event catalogue naming every event and its consumers

#### Scenario: An event is consumed only by the read side

- **WHEN** an event has no write-side consumer but is projected by the read side
- **THEN** the generated catalogue names the read side as its consumer

### Requirement: The generators run in a defined order

Because only the process with the read side configured can name both consumer sets, the read side's generator SHALL build the platform-wide catalogue first, and the write-side generator SHALL be given that catalogue rather than deriving its own.

#### Scenario: Both documents are generated

- **WHEN** the contract generation command runs
- **THEN** the read side's generator runs first and the write-side document embeds the catalogue it produced

#### Scenario: The write-side generator runs alone

- **WHEN** the write-side generator is invoked without being given a catalogue
- **THEN** it falls back to a write-side-only view rather than failing

### Requirement: Committed diagram artifacts are rendered from the code

The event-flow diagram and the saga-step diagram SHALL be rendered from the event and process-manager registries, and SHALL be committed so that they render where the repository is hosted. The generated JSON documents and generated stubs SHALL NOT be committed.

#### Scenario: The diagrams are regenerated

- **WHEN** the diagram command runs
- **THEN** both diagrams are rewritten from the registries

#### Scenario: A generated document is inspected

- **WHEN** the repository is checked out fresh
- **THEN** the diagrams are present and the JSON documents and stubs are not

### Requirement: Drift between a committed artifact and the code fails the build

A check command SHALL fail when a committed artifact no longer matches the code, and the test suite SHALL fail when a generated diagram or a contract document drifts.

#### Scenario: A diagram is stale

- **WHEN** the code changes and a committed diagram is not regenerated
- **THEN** the check fails and the test suite reports the drift

#### Scenario: The diagrams are current

- **WHEN** the committed diagrams match the registries
- **THEN** the check and the tests pass

### Requirement: The gRPC contract offers typed messages and is discoverable

The gRPC contract SHALL define a care service and a garden service. Identifiers SHALL be wrapped types so that one kind of identifier cannot be passed where another is expected, instants and intervals SHALL use the standard well-known types rather than raw integers, and optional fields SHALL express presence explicitly. Health checking and server reflection SHALL be served, so the API can be explored without the definition files.

#### Scenario: A client explores the API

- **WHEN** a client uses reflection against the running server
- **THEN** the services and their messages are discoverable without the definition files

#### Scenario: An identifier is passed to the wrong field

- **WHEN** a client sends an identifier of one type where another is expected
- **THEN** the message does not type-check

#### Scenario: An optional value is absent

- **WHEN** a plant has never been watered
- **THEN** the absent last-watered instant is distinguishable from one that is present

### Requirement: Both protocols dispatch through one application layer

REST and gRPC SHALL resolve to the same command and query handlers and the same dependency container, so that one behaviour exists with two wire formats rather than two implementations.

#### Scenario: The same use case is called over both protocols

- **WHEN** a use case is invoked over REST and over gRPC
- **THEN** both reach the same handler and produce the same domain outcome

### Requirement: Failures map to protocol-specific statuses

Every failure SHALL be reported as a meaningful status for the protocol rather than a generic error. A missing entity, a broken domain rule, a lost concurrency race, a conflicting idempotency key and a malformed value SHALL each map to a documented status. Over gRPC the five are distinct; over REST a missing entity is `404`, a malformed value is `422`, and the broken rule, the lost race and the conflicting key share `409`. The two protocols' mappings SHALL be kept together.

#### Scenario: A domain rule is broken over gRPC

- **WHEN** a request breaks a domain invariant
- **THEN** the client receives the status reserved for a failed precondition

#### Scenario: A concurrency race is lost

- **WHEN** a request loses a concurrency race
- **THEN** the client receives the status reserved for an aborted operation

#### Scenario: A value cannot be interpreted

- **WHEN** a request carries a value that cannot become a domain value
- **THEN** the client receives the status reserved for an invalid argument

### Requirement: The gRPC surface is a second view of the same commands, not a parallel model

The gRPC surface SHALL expose the calls a client makes often and SHALL NOT introduce behaviour the application layer does not already provide. Like the REST process, the gRPC process SHALL NOT contact the broker: its commands commit events to the outbox and the relay publishes them. Stubs SHALL be generated rather than committed.

#### Scenario: A command arrives over gRPC

- **WHEN** a gRPC call changes the domain
- **THEN** the change and its events commit to the outbox and no broker connection is opened

#### Scenario: A fresh checkout is linted

- **WHEN** the project is linted or tested from a fresh checkout
- **THEN** the stubs are generated first, because they are not committed
