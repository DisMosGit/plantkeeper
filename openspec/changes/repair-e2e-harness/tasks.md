# Tasks

## 1. Isolate the suite from events left on the broker

- [x] 1.1 Add a derived event-topic set (`EVENT_TOPICS` values plus the configured raw telemetry topic and `DLQ_TOPIC`) and a function-scoped fixture in `tests/e2e/conftest.py` that deletes each topic's records up to its high-water mark before a test starts its consumers, treating a topic that does not exist yet as already empty; verify with a dedicated `tests/e2e/test_broker_isolation.py` whose first test publishes a plant and whose second asserts a freshly started consumer group is offered none of it, and confirm `uv run pytest tests/e2e/test_broker_isolation.py -q` passes
- [x] 1.2 Make the read side's `running_admin` harness in `tests/e2e/test_cqrs_read_side.py` depend on the isolation fixture, so its projection groups start only after the topics are truncated; verify `uv run pytest tests/e2e/test_cqrs_read_side.py -q` passes and that a second consecutive run of the module passes identically

## 2. Make the worker the suite runs startable

- [x] 2.1 Fix the saga storage binding the worker's entry point resolves: build the process-scoped `ISagaStorage` in `apps/workers/src/plantkeeper/workers/main.py` from the container's session factory, drop the process-scoped provider in `SagaProvider` that dishka cannot hold beside the request-scoped one, and record why in both docstrings; verify `uv run python -m plantkeeper.workers` starts against `make dev` and shuts down on a signal
- [x] 2.2 Add the regression test that would have caught it — the worker's container resolves the binding `build_background_jobs` runs on, at the scope the entry point asks for it, while `ISagaStorage` stays request-bound for a consumer — to `tests/unit/infrastructure/test_providers.py`; verify `uv run pytest tests/unit/infrastructure/test_providers.py -q` passes and fails on the unfixed binding

## 3. One shared harness for the worker side

- [x] 3.1 Add a shared write-side harness fixture in `tests/e2e/conftest.py` that starts the outbox relay, the write-side consumer groups, the telemetry ingress and the `CommandDispatcher` selected from `build_background_jobs` — the interval-driven timers deliberately excluded — stops all of them with the fixture, and depends on the isolation fixture; verify `uv run pytest tests/e2e/test_saga_flow.py::test_adding_a_plant_onboards_it -q` passes with that module using the shared fixture
- [x] 3.2 Migrate `test_journal_flow.py`, `test_provenance.py` and `test_iot_flow.py` to the shared harness and delete their local worker context managers; verify `uv run pytest tests/e2e/test_journal_flow.py tests/e2e/test_provenance.py tests/e2e/test_iot_flow.py -q` passes, including the journal tests that now depend on the dispatcher executing the recorded care command
- [x] 3.3 Migrate `test_long_polling.py`, `test_notification_stream.py`, `test_dlq_replay.py` and `test_catalog_sync.py` to the shared harness and delete every remaining local copy; verify `grep -rn "async def running_worker" tests/e2e` finds only the shared definition in `conftest.py` and `uv run pytest tests/e2e -q` passes
- [x] 3.4 Update the prose that describes how the suite runs the worker — the harness docstrings, `docs/events.md` where it names the e2e consumer groups, and `CONTRIBUTING.md` if it states the test workflow — to name the shared harness and the per-test isolation rule; verify `uv run pytest tests/unit/docs -q` passes and every changed relative link resolves

## 4. Integration check

- [x] 4.1 Run the whole suite twice in succession on the same working tree and confirm both runs pass the same set of tests with no failures; verify `make test` exits zero on both runs and `make test-unit` remains green
- [x] 4.2 Run the full gates — `make contracts`, `uv run python tools/contracts.py --check`, `make lint`, `make test`, `make coverage` — and confirm all exit zero with the per-layer coverage reports still above their floors; verify the three previously failing schedule tests, the provenance test and the long-polling test all pass
- [x] 4.3 Re-run the gate that this change unblocks: `remediate-audit-findings` task 12.2, and mark that task complete in `openspec/changes/remediate-audit-findings/tasks.md` once its gates pass
