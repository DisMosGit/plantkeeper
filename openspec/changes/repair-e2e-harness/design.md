# Design

## Context

See `proposal.md` — Why, for the failures themselves. Three properties of the current
suite shape the approach:

- **Nine modules hand-roll the same harness.** `test_saga_flow`, `test_journal_flow`,
  `test_provenance`, `test_iot_flow`, `test_long_polling`, `test_notification_stream`,
  `test_dlq_replay` and `test_catalog_sync` each define their own
  `running_worker`/`running_worker_consumers` async context manager, and
  `test_cqrs_read_side` defines the read side's `running_admin`. All eight write-side
  ones register the write-side consumers, four also register the telemetry ingress and
  five run the relay; none starts a background job. There is no single place where "what
  the worker runs" is stated for the suite, which is exactly why the dispatcher could go
  missing from all eight.
- **The broker is session-scoped and the groups are not.** `tests/conftest.py` starts
  one Kafka container for the whole suite and hands every test the same bootstrap
  address, while `worker_settings`/`read_side_settings` mint a fresh consumer-group
  prefix per test and every registration uses `auto_offset_reset="earliest"`. A fresh
  group over a non-empty topic therefore reads the whole topic, not the test's part of
  it.
- **The ledger cannot save the suite.** Consumers deduplicate on
  `(consumer_group, event_id)`, but a fresh group has an empty ledger by construction,
  so replay protection is precisely what a per-test group does not have.
- **The worker those components belong to does not start.** `SagaProvider` declares
  `ISagaStorage` twice — `Scope.APP` for the recovery job, `Scope.REQUEST` for the
  consumers — and its docstring reads as if the container chooses by scope. It does
  not: a container holds one factory per type, the request-scoped one wins, and the
  process-scoped lookup `workers/main.py` makes to build its job list raises
  `NoFactoryError`. Nothing caught it: the worker's entry point has no test, and
  every other caller resolves the storage inside a request scope.

## Goals / Non-Goals

**Goals:**

- One shared definition of the worker-side process the e2e suite runs, so that the
  suite cannot silently diverge from the workers target again.
- Isolation that is a property of the suite, not of each test, and that derives its
  topic coverage from the event catalogue.
- Keep the change to the suite and the one binding that has to work for it: no runtime
  setting, schema or contract changes.

**Non-Goals:**

- Running the interval-driven timers in the shared harness. A `BackgroundJob`'s
  `run()` ticks once immediately and only then waits (`command_dispatch.py:67-78`), so
  a long interval would not stop the first tick; a test that needs a timer keeps
  driving it explicitly, as `test_iot_flow` already does.
- Making the e2e suite parallel-safe. The isolation fixture assumes tests run one at a
  time, which is how `make test` runs them.
- Changing any production behaviour, or making the worker configurable for tests. The
  one production edit below restores a binding the worker used to resolve; it adds no
  behaviour.

## Decisions

### 1. A shared harness in `tests/e2e/conftest.py`, not a per-module context manager

The fixture yields while the worker-side process is up, and starts four things
together:

- `register_consumers(broker, ...)` — the write-side consumer groups;
- `register_telemetry_ingest(broker, ...)` — the `telemetry.raw` ingress, which some
  flows need and no module should have to remember;
- the `OutboxRelay`, run as a task for the fixture's lifetime and stopped with it — the
  flows whose pipeline crosses the outbox twice cannot finish without it, and five of
  the eight deleted copies ran it;
- the production `CommandDispatcher` from `build_background_jobs`, run as a task for
  the fixture's lifetime and stopped with it.

The dispatcher is taken from `build_background_jobs` and selected by type rather than
constructed inline, so the harness reads the production job list: if that list changes,
the selection is the one line that fails to compile rather than eight modules that keep
passing while asserting against a process that no longer exists. (The production list
also contains the four timers; selecting from it *is* how they are deliberately
excluded — see Non-Goals.) The read side's `running_admin` harness keeps its own
registration but takes the same shared definition and depends on the same isolation,
so the two sides cannot drift apart.

**Alternatives considered.** *Add the dispatcher to the three failing modules only* —
smallest diff, but leaves eight copies to drift and does not fix the class of defect.
*Run every job from `build_background_jobs`* — the highest fidelity, rejected because
each job's first tick fires immediately: the missed-care tick and the silence timer
would then be producing notifications inside unrelated tests, which is the interference
this change exists to remove.

### 2. Isolation by truncating the event topics before each test

A function-scoped fixture deletes the records already on every event topic, up to each
topic's high-water mark, through an `aiokafka` `KafkaAdminClient`. A fresh consumer
group's `earliest` then resolves to the deletion point, so a test's group is offered
only what that test published. The fixture is a dependency of the shared harness, which
is where consumer groups are created, so no test opts in and no test can forget.

The topic set is derived, not listed: the values of `EVENT_TOPICS`, plus the configured
raw telemetry topic, plus `DLQ_TOPIC`. A topic added to the catalogue is isolated
without an edit, which is the scenario the spec asks for. A topic that does not exist
yet (nothing has published to it) raises `UnknownTopicOrPartitionError`; the fixture
treats that as "already empty" rather than an error.

**Alternatives considered.** *Make the offset reset a setting and use `latest` for test
groups* — adds a product setting for a test concern, and races: the group must be
assigned before the test produces, and `broker.start()` does not guarantee assignment.
*Give every test its own topic namespace* — topics are module constants woven through
the registry, the contracts and the docs; the blast radius is the whole messaging layer
for a test-only benefit. *Filter each assertion by the test's own identifiers* —
removes the symptom, keeps the interference, and contradicts the requirement that a
test observes only its own events.

### 3. Consolidate rather than duplicate, in one commit-sized step

The eight local harnesses are replaced by the shared fixture in the same change. The
per-module context managers differ only in whether the ingress is registered and in
their docstrings, so consolidation is mechanical; the suite passing is the evidence
that no module depended on a detail of its own copy.

### 4. Build the process-scoped saga storage in the worker's entry point

The harness cannot start the dispatcher — or anything else `build_background_jobs`
returns — without the storage that function is handed, and the production way to get it
is the lookup that raises. The fix is in `workers/main.py`, not in the fixture: the
fixture building its own storage would let the suite pass while `make workers` stayed
unrunnable, which is the exact kind of divergence this change exists to remove.

The unbound storage is built from the container's session factory, next to the job list
— which that file already builds by hand for the same reason, "each one opens request
scopes of its own". The container keeps the request-scoped binding, which is the one
that matters for correctness: a consumer's saga step commits its checkpoint with the
delivery's unit of work. Both callers keep what they had before the two bindings were
declared:

- `SagaRecoveryJob` reads and updates status through the unbound storage it is given;
- its `_park` resolves `ISagaStorage` inside a request scope, where the request-bound
  one is what the counter and the outbox row have to share a transaction with.

**Alternatives considered.** *Keep both providers and pick by scope* — what the code
does today, and not something this container supports: with the request-scoped provider
last the process-scoped lookup raises, and with it first *both* scopes resolve to the
unbound storage, which would quietly take the consumers off their transaction.
*Give the process-scoped binding its own key* — a second protocol or a named component
for one caller, and a new concept in the container to explain. *Resolve the storage in
the fixture instead* — rejected above.

### 5. A unit test on the resolution, because the process itself has none

The regression was invisible because nothing resolved that binding at that scope:
every other caller asks inside a request. The test added to
`tests/unit/infrastructure/test_providers.py` builds the worker's container and asks
for exactly what the entry point asks for, and for the request-bound binding a consumer
gets, so the two cannot collapse into one again. It is a unit test — no container, no
broker — because what is under test is the wiring, not the worker's runtime.

## Risks / Trade-offs

- **[Truncating records could remove something a test needs]** → No e2e test consumes a
  message published before it starts its consumers; the pattern throughout is
  publish-then-consume inside one test. The fixture runs before the harness starts, so
  a test's own publishes are never in scope.
- **[Isolation could hide a genuine consumer-order problem]** → It removes only
  *inter-test* interference. Within a test, ordering assertions (the relay's key
  barrier, the saga's step order) are untouched, because those messages are the test's
  own.
- **[A per-test admin call adds latency]** → One `delete_records` per topic per test,
  against a local single-node broker; the suite already spends tens of seconds waiting
  on consumer flow, so this is noise.
- **[The harness now runs a poller for every consumer test]** → The dispatcher's
  interval defaults to one second and it is stopped with the fixture; its first tick
  finds no intents in a test that records none, which is a no-op read.
- **[The dispatcher in-process can write where a test does not expect]** → It only
  executes intents a saga recorded in that test's own database. With isolation in
  place, no other test's saga can record one.
- **[Deriving topics from `EVENT_TOPICS` misses a non-domain topic]** → The raw
  telemetry topic and the dead-letter topic are added explicitly; both are settings or
  constants, not literals copied into the fixture.
- **[A test-repair change now edits production wiring]** → One expression moves into
  the composition root that already builds the job list by hand, and the provider it
  replaces could not be resolved at all. It is covered twice: the unit test on the
  resolution, and every e2e test that now needs the dispatcher that resolution builds.
  Anything wider — a setting, a schema, a contract — stays out of this change.

## Migration Plan

Nothing to deploy: the fixtures and the eight modules change together, `make test` and
`make coverage` are the verification, and reverting the change restores the previous
harnesses. The worker is startable again — its entry point builds the storage it used
to resolve before the second binding was added — which is verified by starting it once
against the compose stack, not only by the suite. `remediate-audit-findings` task 12.2
is then re-run to confirm its gate passes.
