# Tasks

## 1. Message provenance and schema version

- [x] 1.1 Add `correlation_id`, `causation_id`, `raised_by`, `schema_version` and `traceparent` to the outbox model and the outbox message type, with an additive Alembic migration; verify `tests/integration/test_migrations.py` passes and `make migrate` applies cleanly on an empty database
- [x] 1.2 Make the publisher emit the new headers and the decoder tolerate their absence, treating missing provenance as unknown origin; verify the header set and the tolerance in `tests/unit/infrastructure/test_relay.py` and `tests/unit/infrastructure/test_decoding.py`
- [x] 1.3 Propagate provenance end to end: the API mediator stamps requests it dispatches, and every consumer copies correlation and causation from the delivery onto the events it appends to the outbox; verify unit tests cover an API command, a consumer reaction and a saga step each producing a correctly stamped outbox row
- [x] 1.4 Document the envelope in `docs/events.md` and add the ADR recording the provenance fields, the schema-version policy and the accepted no-auth trust model; verify `make contracts` regenerates the AsyncAPI documents with the new headers and `uv run python tools/contracts.py --check` passes

## 2. Outbox relay claiming and per-key ordering

- [x] 2.1 Claim outbox rows with `FOR UPDATE SKIP LOCKED` and a stale-lease expiry so several relays can run at once, with the columns in the same migration as 1.1 where practical; verify an integration test in `tests/integration/test_repositories.py` shows two concurrent fetchers never receive the same row and a crashed lease is reclaimed
- [x] 2.2 Hold a partition key back while an older row of that key is unpublished, and release it when the older row is published or abandoned; verify an integration test that injects a publish failure and asserts the later events of that key are not published before the failed one, while other keys proceed
- [x] 2.3 Update the transport and failure-handling sections of `docs/events.md` for claiming and the ordering guarantee; verify `tests/unit/docs` and `uv run python tools/contracts.py --check` pass

## 3. Saga checkpoint commits with the step (P0)

- [x] 3.1 Bind the saga storage run to the unit of work's session so a step's effect, its `saga_log` entry, its `saga_state` checkpoint and its lifecycle outbox row commit in one transaction; verify an integration test in `tests/integration/test_sagas.py` shows a failure after the step leaves neither an effect nor a log entry, and a success leaves all four together
- [x] 3.2 Commit `SagaStarted`, `SagaCompleted`, `SagaFailed` and `SagaCompensated` in the transaction of the state change they report; verify a test that compares `write_shared.saga_state` with the lifecycle rows in `write_shared.outbox` after a crash between steps and finds them consistent
- [x] 3.3 Update `docs/sagas.md` and ADR 0005 to remove the step/checkpoint crash window from "Known limitations" and describe the single-transaction checkpoint; verify the documented inspection SQL in `docs/sagas.md` runs against the test database as written

## 4. Recorded cross-context commands and retryable failures (P1)

- [x] 4.1 Add `write_shared.saga_intents` (saga id, step number, command name, payload, status, attempts, lease) and its repository behind an application port, with a migration; verify `tests/integration/test_migrations.py` covers the table and a repository round-trip test passes
- [x] 4.2 Make the onboarding steps record commands for the care and notification contexts in the step's transaction instead of writing those tables, keeping the publishing step last; verify unit tests show each step stages an intent and performs no cross-context write, and `make lint`'s import rules still pass
- [x] 4.3 Add the command-dispatcher consumer to `make workers`: claim intents, execute the owning context's handler through its own unit of work, and mark the intent executed in that transaction; verify an integration test shows a crash between recording and executing an intent yields exactly one effect on retry, and that a duplicated delivery of the same intent is a no-op
- [x] 4.4 Split the recorded failure into retryable and parked: a recorded failure is re-run from its step history within a bounded budget, the budget exhausted parks the process, and an operator reset clears the budget; add the new lifecycle events to the domain catalogue with `tests/unit/domain/test_events_catalogue.py` updated; verify tests cover retry-after-failure, park-after-budget and reset-to-running
- [x] 4.5 Record the decision in a new ADR and rewrite the saga sections of `docs/sagas.md` and `docs/events.md` for recorded commands and the failure budget; verify `make contracts` and `tests/unit/docs` pass with the new consumer group and events visible in the generated documents

## 5. Event-maintained cross-context references

- [x] 5.1 Add a reference table per consuming context (notifications and journal) filled from `garden.events` under each consumer's own group, and replace the shared-session `plant_id` lookups in the notification and journal consumers with reads of their own reference rows; verify integration tests show a fact for a not-yet-known plant is held or dropped per the documented rule, and no query in those consumers reads a `write_garden` table
- [x] 5.2 Remove the catalogue-lookup and plant-resolution exceptions from `docs/architecture.md` and `docs/sagas.md`, stating instead that references are event-maintained; verify `make lint` and the integration suite pass with the shared session used only inside a context

## 6. Terminal path for failing consumers

- [x] 6.1 Classify consumer failures: a broken domain rule is moved aside immediately, an infrastructure failure is retried a bounded number of times with backoff, and a delivery past its budget is copied to `plantkeeper.dlq.v1` with its consumer group, origin and error in headers while its ledger claim commits with the copy; verify integration tests cover the poison delivery, the transient failure that recovers, and the partition continuing past a moved-aside delivery
- [x] 6.2 Replace the recorded-gap wording in `docs/events.md` with the retry and dead-letter policy and the runbook for replaying a moved-aside delivery; verify the documented replay command works against the test stack and `tests/unit/docs` passes

## 7. Read side on its own database

- [x] 7.1 Add a second Postgres to `docker-compose.yml`, point the Django read schema and a read-only SQLAlchemy engine at it through settings and `.env.example`, and keep both URLs configurable to the same instance for small machines; verify `make dev` starts both healthy, `make migrate` applies the write and read schemas to their own instances, and `tests/integration/test_migrations.py` covers both
- [x] 7.2 Document the topology change in `docs/architecture.md` and the runbooks, including the rollback of pointing both URLs at one instance; verify every documented command runs as written against the compose stack

## 8. Client queries answered from read models

- [x] 8.1 Add the read-model query port and move the list and report queries — plants, care schedules, species list, journal timeline — onto it, keeping command answers, command-support reads, the journal's as-of-date replay and the notification delivery endpoints on the write side; verify unit tests per moved query and an end-to-end test showing a list query works with the write database unreachable
- [x] 8.2 Document the query split and the staleness window in `docs/cqrs.md` and in the affected OpenAPI descriptions; verify `make contracts` regenerates the document and the drift tests pass

## 9. Telemetry rollup read side

- [x] 9.1 Add the `read_telemetry` schema with rollup and latest-per-sensor models and their Django migration, plus read-only SQLAlchemy mappings with a parity test; verify the migration applies to the read instance and the parity test fails when the two mappings disagree
- [x] 9.2 Add the telemetry rollup projection: its own consumer group and ledger, window upserts with out-of-order recompute, and rebuild by replay; verify `tests/integration/test_projections.py` covers a window update, an out-of-order reading and a full rebuild
- [x] 9.3 Shrink the unprojected-events list in `tests/integration/test_projections.py`, and update `docs/telemetry.md` and `docs/cqrs.md` for the new read side and its rebuild procedure; verify the list test and `tests/unit/docs` pass

## 10. Readings retention and the silent sensor

- [x] 10.1 Extend the partition job to drop whole month partitions older than the documented retention window while rollups survive; verify an integration test shows an aged partition is dropped and a late reading still lands in the catch-all partition
- [x] 10.2 Persist `last_seen_at` with each stored reading and add the silence timer that publishes `SensorOffline` once per silence, with the notification consumer creating the `sensor_offline` reminder from it; verify integration and end-to-end tests cover one event per silence, no event inside the period, and a new event after the sensor reports and goes quiet again, and update `docs/telemetry.md`, `docs/events.md` and `docs/notifications.md` for the new producer

## 11. Streaming notifications

- [x] 11.1 Add the SSE endpoint over the existing household signal: resume from a cursor, fan out one subscription per household, and hold no database session while idle; verify end-to-end tests cover a notification appearing on an open stream, a reconnect resuming from the cursor, and the stream degrading to a plain read when the signal is unavailable
- [x] 11.2 Keep the request-and-wait endpoint unchanged as the fallback and document both forms in `docs/notifications.md` and the OpenAPI descriptions; verify `make contracts` and the notifications test suite pass with both endpoints present

## 12. Claims, contracts and full gates

- [x] 12.1 Rewrite the README and `docs/architecture.md` where they overstate the architecture — integration between processes is events, sagas coordinate through recorded commands, the query split and the two-database topology are stated, and the telemetry gap is gone — and verify no tracked document contradicts the updated specs
- [x] 12.2 Regenerate the contracts and diagrams (`make contracts`) and run the full gates — `make lint`, `make test`, `make coverage` — and verify all pass with `uv run python tools/contracts.py --check` clean
