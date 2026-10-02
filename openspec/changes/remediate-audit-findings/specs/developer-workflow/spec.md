# Spec Delta

## MODIFIED Requirements

### Requirement: Local infrastructure and application processes are started separately

One command SHALL start the local broker, the write database, the read database and the cache and SHALL wait until they report healthy. That command SHALL NOT start any application process.

#### Scenario: Infrastructure is started

- **WHEN** a contributor runs the infrastructure command
- **THEN** the broker, both databases and the cache are running and healthy, and no application process has been started

#### Scenario: Infrastructure is stopped

- **WHEN** a contributor stops the infrastructure
- **THEN** the services stop and their data is retained unless removal was explicitly requested

### Requirement: Every runnable process has a named target

Each process SHALL be startable by name: the REST API, the gRPC server, the write-side workers, the read side, and the telemetry simulator. The workers target SHALL run the relay, the write-side consumers — including the dispatcher that executes recorded cross-context commands — and the timers in one process, and the read-side target SHALL run the projections and the administrative view in one process.

#### Scenario: The write-side back end is started

- **WHEN** a contributor starts the workers target
- **THEN** the relay, the write-side consumer groups and the timers — including the sensor silence timer — run in a single process

#### Scenario: The read side is started

- **WHEN** a contributor starts the read-side target
- **THEN** the domain projections, the telemetry rollup projection and the administrative view are served together

#### Scenario: An unavailable process is not implied

- **WHEN** a contributor reads the available targets
- **THEN** each target that exists is documented, and no target is documented that does not exist
