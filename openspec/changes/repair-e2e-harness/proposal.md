# Proposal

## Why

The end-to-end suite stopped being a faithful replica of the processes it asserts
against, and it now fails four or five tests per run because of it.

Two independent defects, both introduced by `remediate-audit-findings`' own (correct)
design decisions:

- **The harness runs half of `make workers`.** Eight e2e modules each hand-roll a
  worker harness that calls `register_consumers` (and, in some, the telemetry ingress or
  the relay) but never `build_background_jobs`. Since onboarding began recording its
  cross-context care command for a dispatcher to execute rather than writing the care
  table, three tests wait thirty seconds for a care schedule that no started component
  can write.
- **Tests see each other's events.** The Kafka container is session-scoped while every
  e2e test joins it with a brand-new consumer group at `auto_offset_reset="earliest"`
  over freshly truncated tables, so each test replays every earlier test's events. A
  saga then starts for a plant whose rows were truncated, commits a `SagaStarted` into
  the next test's outbox, and the next test asserts against it; a replayed silence
  becomes an unexpected notification. The failure set is not even stable: the same
  suite failed five tests in one run and four in the next.

`make test` and `make coverage` therefore cannot be green, which is why
`remediate-audit-findings` cannot finish its own task 12.2 — and, more importantly, why
the suite can no longer be trusted as evidence that a cross-process flow works.

Wiring the shared harness then uncovered a third, harder defect:

- **The worker the harness has to run does not start.** `make workers` — and the e2e
  harness with it — crashes before it subscribes to anything, because the job list it
  builds needs the process-scoped `ISagaStorage` and that lookup raises. The same
  commit that added the request-scoped binding for the consumers declared the
  process-scoped one beside it, on the assumption that the DI container picks between
  them by scope; it does not, and the process-scoped binding is unreachable. A harness
  that ran the dispatcher would be asserting against a process that cannot exist in
  production, so the repair is part of this change rather than a workaround inside it.

## What Changes

- **One shared e2e harness** replaces the eight per-module copies of the write-side
  worker. It starts the components the workers target runs for the flow under test — the
  outbox relay, the write-side consumer groups, the telemetry ingress, and the
  `CommandDispatcher` that executes a saga's recorded cross-context commands — so a
  component added to the workers target has one place to be added to the suite instead of
  eight places to be forgotten. The interval-driven timers stay out of it: a test that
  needs one drives it explicitly, as the silence test already does. The read side's own
  admin harness takes the same isolation and settings fixtures, so the two sides cannot
  drift apart either.
- **Per-test broker isolation.** An e2e fixture removes the records already on the
  domain topics before a test starts its consumers, so a fresh consumer group replays
  the test's own events and nothing else. A previous test's delivery can then neither
  satisfy nor disturb an assertion.
- **The suite asserts against the process it claims to run**, so the docstrings that say
  "as `make workers` would" become true, and `make test` / `make coverage` become green.
- **The worker starts again.** The process-scoped saga storage a background job runs on
  is built by the worker's own entry point, next to the job list it already builds
  there, and the process-scoped provider that could never be resolved is gone. Both
  bindings stay: consumers keep the request-bound storage that makes a step's
  checkpoint part of their transaction, and the recovery job keeps the unbound one it
  reads and updates status through.
- **No other runtime behaviour changes.** The API, the read side, the schemas and the
  contracts are untouched; the fix above returns the worker to what it did before the
  binding was added.

## Capabilities

### New Capabilities

None. Test-suite behaviour belongs to the capability that already describes how a
contributor validates this repository.

### Modified Capabilities

- `developer-workflow`: the end-to-end suite is required to start the components it
  asserts against, through one shared harness, and to keep each test's events to itself
  — which requires the worker whose components it runs to be startable, so the worker's
  entry point resolves every dependency its job list needs.

## Impact

- `tests/e2e/conftest.py` — the shared worker-process fixture and the broker-isolation
  fixture.
- `tests/e2e/test_saga_flow.py`, `test_journal_flow.py`, `test_provenance.py`,
  `test_iot_flow.py`, `test_long_polling.py`, `test_notification_stream.py`,
  `test_dlq_replay.py`, `test_catalog_sync.py` — use the shared harness; their local
  worker context managers are deleted. `test_cqrs_read_side.py` — its admin harness
  shares the same definition and the same isolation. `tests/e2e/test_broker_isolation.py`
  — the new test of the isolation itself.
- `apps/workers/src/plantkeeper/workers/main.py` — the process-scoped saga storage,
  built where the job list is built.
- `packages/infrastructure/src/plantkeeper/infrastructure/di/providers.py` — the
  process-scoped `ISagaStorage` provider that dishka could not resolve is removed.
- `tests/conftest.py` — the topic set the isolation fixture truncates is derived from
  `EVENT_TOPICS` plus the raw telemetry topic and the dead-letter topic, so a new topic
  cannot be forgotten. `tests/unit/infrastructure/test_providers.py` — the regression
  test for the binding above.
- `docs/` — `docs/events.md` or `CONTRIBUTING.md` if either states how the e2e suite
  runs the worker.
- Unblocks `remediate-audit-findings` task 12.2; no schema, contract or API surface is
  affected.
