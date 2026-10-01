# developer-workflow Specification

## Purpose

How a contributor installs, runs and validates this repository, how the architecture is enforced by tooling rather than by review, and the change lifecycle every modification follows.

## Requirements

### Requirement: The whole workspace installs with one command

The repository SHALL be a single workspace whose members are the packages, the applications and the tools it contains, and one install command SHALL install all of them. The repository root SHALL NOT itself be an installable package.

#### Scenario: A contributor installs the project

- **WHEN** a contributor with the required interpreter and package manager runs the install command
- **THEN** every workspace member's dependencies are installed

#### Scenario: The root is built

- **WHEN** the workspace is built
- **THEN** the root contributes no package of its own

### Requirement: Local infrastructure and application processes are started separately

One command SHALL start the local broker, database and cache and SHALL wait until they report healthy. That command SHALL NOT start any application process.

#### Scenario: Infrastructure is started

- **WHEN** a contributor runs the infrastructure command
- **THEN** the broker, database and cache are running and healthy, and no application process has been started

#### Scenario: Infrastructure is stopped

- **WHEN** a contributor stops the infrastructure
- **THEN** the services stop and their data is retained unless removal was explicitly requested

### Requirement: Every runnable process has a named target

Each process SHALL be startable by name: the REST API, the gRPC server, the write-side workers, the read side, and the telemetry simulator. The workers target SHALL run the relay, the write-side consumers and the timers in one process, and the read-side target SHALL run the projections and the administrative view in one process.

#### Scenario: The write-side back end is started

- **WHEN** a contributor starts the workers target
- **THEN** the relay, the write-side consumer groups and the timers run in a single process

#### Scenario: The read side is started

- **WHEN** a contributor starts the read-side target
- **THEN** the projections and the administrative view are served together

#### Scenario: An unavailable process is not implied

- **WHEN** a contributor reads the available targets
- **THEN** each target that exists is documented, and no target is documented that does not exist

### Requirement: Linting and testing are one command each

Linting SHALL run the linter, the formatter's check mode, the strict type checker and the architectural import rules, as a single command. Testing SHALL run the test suite as a single command. Both SHALL generate the artifacts the code depends on before running.

#### Scenario: Lint is run

- **WHEN** a contributor runs the lint command
- **THEN** linting, formatting, type checking and the import rules are all checked

#### Scenario: Tests are run from a fresh checkout

- **WHEN** a contributor runs the test command from a fresh checkout
- **THEN** the artifacts the tests import are generated first

### Requirement: The architecture is enforced by tooling

The dependency rules SHALL be enforced mechanically, and a violation SHALL fail the build rather than be caught in review. The domain layer SHALL depend on nothing outside itself, the application layer on the domain, the infrastructure layer on the domain and the application, the applications on all of them, and the telemetry simulator on none of them. No bounded context SHALL import another context's internals.

#### Scenario: The domain imports infrastructure

- **WHEN** the domain layer imports a persistence, web or messaging library
- **THEN** the import rules fail the build

#### Scenario: One context imports another

- **WHEN** a bounded context imports another context's internals
- **THEN** the import rules fail the build

#### Scenario: The simulator imports an application layer

- **WHEN** the telemetry simulator imports the domain, application or infrastructure packages
- **THEN** the import rules fail the build

### Requirement: Coverage is reported per layer against documented floors

Test coverage SHALL be reported for the domain, application and infrastructure layers against the floors of 90, 80 and 70 percent respectively, and the domain layer's dedicated run SHALL fail when it falls below its floor. A single global coverage threshold SHALL NOT be configured, so that covering one layer at a time remains possible.

#### Scenario: Coverage is measured

- **WHEN** the coverage command runs
- **THEN** each of the three layers is reported against its documented floor

#### Scenario: The domain floor is missed

- **WHEN** the domain layer's dedicated test run falls below its floor
- **THEN** the run fails

#### Scenario: One layer is tested alone

- **WHEN** a contributor runs the tests for a single layer
- **THEN** the run is not failed by the other layers' coverage

### Requirement: Generated artifacts are regenerated, never edited

Generated stubs, contract documents and the committed diagrams SHALL be produced from the code, and a stale committed artifact SHALL be regenerated rather than hand-edited. Schema changes SHALL be applied by a single command that runs both the write side's and the read side's migrations.

#### Scenario: A contract changes

- **WHEN** a change alters a contract
- **THEN** the artifact is regenerated from the code rather than edited

#### Scenario: Migrations are applied

- **WHEN** a contributor runs the migration command
- **THEN** both the write-side and the read-side schemas are brought up to date

### Requirement: The same gates run as commit hooks

The pre-commit hooks SHALL run the linter with autofix, the formatter, and the project-wide strict type check, together with basic file hygiene checks. Hooks SHALL NOT be bypassed.

#### Scenario: A commit is made

- **WHEN** a contributor commits
- **THEN** the hooks run and a failure stops the commit

#### Scenario: A hook fails

- **WHEN** a hook reports a problem
- **THEN** the cause is fixed and the change is staged again rather than the hook being bypassed

### Requirement: Commits state what changed and why

Commit messages SHALL follow the conventional form of a type and a scope followed by a summary. The type and scope SHALL be lowercase and the summary SHALL be an imperative phrase on a single line with no trailing period. A commit SHALL carry a body explaining why the change was made, wrapped for reading. Commits SHALL NOT carry co-author or tool-attribution trailers, and SHALL NOT be amended once they may have been published.

#### Scenario: A commit is written

- **WHEN** a contributor commits a change
- **THEN** the subject fits the conventional form and the body explains the reasoning behind the change

#### Scenario: A scope is chosen

- **WHEN** a contributor picks a scope
- **THEN** the scope comes from the documented list covering the bounded contexts, the layers and the cross-cutting areas

#### Scenario: A commit is wrong

- **WHEN** a contributor notices a mistake in an unpushed commit
- **THEN** it is corrected, and a published commit is never rewritten

### Requirement: Every change follows the OpenSpec lifecycle

A change SHALL be proposed as an OpenSpec change carrying its planning artifacts before implementation begins, SHALL be validated, SHALL be implemented one task at a time, and SHALL be archived once complete so that its specification deltas become part of the project's specifications.

#### Scenario: A contributor starts work

- **WHEN** a contributor wants to change behaviour
- **THEN** a change with its planning artifacts is created before code is written

#### Scenario: A change is validated

- **WHEN** a change's artifacts are complete
- **THEN** validation passes in its strict mode

#### Scenario: A change is implemented

- **WHEN** a task of a change is finished
- **THEN** it is marked complete in the same commit as the work it describes

#### Scenario: A change is finished

- **WHEN** every task of a change is complete and the gates pass
- **THEN** the change is archived and its deltas are merged into the main specifications

### Requirement: Tests accompany the change that needs them

Behaviour SHALL be covered by tests in the same change that introduces it: domain logic by a unit test, input and output by an integration test, and a flow that crosses processes by an end-to-end test. Test levels SHALL be selectable by marker, and the markers SHALL be declared so an undeclared marker fails the run.

#### Scenario: Domain logic is added

- **WHEN** a change adds or alters an invariant
- **THEN** a unit test covers it in the same change

#### Scenario: Input and output are added

- **WHEN** a change adds persistence or messaging behaviour
- **THEN** an integration test covers it

#### Scenario: An undeclared marker is used

- **WHEN** a test uses a marker the project has not declared
- **THEN** the test run fails

### Requirement: The project stays out of scope deliberately

The project SHALL NOT introduce continuous integration or deployment pipelines, container orchestration, infrastructure-as-code, or authentication, and SHALL NOT introduce an alternative task queue or a synchronous call between bounded contexts.

#### Scenario: A change proposes a pipeline

- **WHEN** a change would add a continuous integration configuration or a deployment system
- **THEN** it is out of scope for this project

#### Scenario: A change proposes authentication

- **WHEN** a change would add authentication to the platform
- **THEN** it is out of scope, because the platform models no login
