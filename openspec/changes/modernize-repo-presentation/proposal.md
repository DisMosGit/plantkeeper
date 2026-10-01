# Proposal

## Why

The repository's presentation has drifted behind the system it describes.

`ROADMAP.md` is 100 KB of Russian planning prose holding **358 completed tasks and zero
pending ones** — v0.1.0 shipped and every Definition of Done is checked, so the file is
history that git already keeps. `CHANGELOG.md` duplicates what GitHub Releases records
and is maintained by hand. `.agents/skills/commit/SKILL.md` is still
`adapted-for: numenews`: it names another project's packages, secret sources, coverage
floors and build gates, so an agent following it would run the wrong commands here. The
README has no badges and never says where release notes live, there is no `.github/`
scaffolding, and `openspec/specs/` is empty — which means no future change has a
baseline to write a delta against.

This change removes the two stale root documents, repairs every reference to them, makes
the commit skill describe *this* project, refreshes the repository's front door, and
backfills the capability baseline that OpenSpec needs to be useful.

## What Changes

- **BREAKING (documentation links)**: delete `ROADMAP.md` and `CHANGELOG.md` from the
  repository root. Release history moves to GitHub Releases; the roadmap's still-relevant
  working rules move into `CONTRIBUTING.md`.
- Backfill a **baseline capability spec** for every shipped capability into
  `openspec/specs/`, so the first real change has something to modify. This is the
  roadmap's Definition of Done expressed as testable requirements instead of completed
  checkboxes.
- Repair every reference to the two deleted files: 10 files reference them today.
- Rewrite `.agents/skills/commit/SKILL.md` for PlantKeeper — correct workspace layout,
  scopes, secret sources, ignored paths, generated artifacts and build gates, plus the
  OpenSpec-aware commit sequencing that already exists in the file.
- Refresh `README.md`: badges, a plain-language description, key features, quick start,
  how OpenSpec is used here, repository layout, contributing, license, and a pointer to
  GitHub Releases.
- Rewrite `CONTRIBUTING.md` around the OpenSpec lifecycle (propose → review → implement →
  archive) and drop the "update `CHANGELOG.md`" step.
- Add minimal `.github/` issue and pull-request templates that reference OpenSpec.
- **Deliberately skipped**: `CODE_OF_CONDUCT.md` and `SECURITY.md`. This is a
  single-maintainer pet project with no authentication, no users and no security
  surface, so both would be ceremony. Nothing links to them either.

## Capabilities

### New Capabilities

The project has no specs yet, so every capability below is new. Eleven of them describe
the platform as it already ships (the backfill); the last one describes the repository
presentation this change introduces.

- `plant-collection`: households and plants — registration, lifecycle, sensor
  association, and the invariants the Garden context enforces.
- `care-scheduling`: care schedules and their execution — what makes care due, how
  completion is recorded, and how adaptive watering responds to telemetry.
- `care-journal`: the event-sourced Journal — append-only streams, snapshots, replay,
  and the protection around a plant's stream.
- `species-catalog`: the species catalogue and its Trefle anti-corruption layer —
  synchronisation, circuit breaking, caching, and the catalogue's read model.
- `iot-telemetry`: the raw telemetry stream, its ingress and deduplication, the
  partitioned readings table, the anomaly events it emits, and the simulator that
  produces it.
- `notifications`: notification creation, the Valkey wake-up channel, and the HTTP long
  polling delivery protocol.
- `read-models`: the CQRS read side — idempotent projections, the `read_analytics`
  schema, read-only Django Admin, and how a read model is rebuilt.
- `process-managers`: the four sagas — which coordinate and which choreograph, their
  compensations, their timers, and their recovery.
- `event-transport`: the transactional outbox, the relay, Kafka topics and the event
  envelope, and the idempotency contract every consumer owes.
- `integration-contracts`: the generated REST, gRPC and AsyncAPI contracts, the checked-in
  diagrams rendered from code, and the tests that fail when either drifts.
- `developer-workflow`: the uv workspace layout, local infrastructure, the `make`
  surface, the lint/test/coverage gates, and the OpenSpec change lifecycle this
  repository follows.
- `repository-presentation`: what the repository shows a visitor — README, contributing
  guide, issue and pull-request templates, the commit skill, and the rule that release
  history lives in GitHub Releases rather than a root changelog.

### Modified Capabilities

None. `openspec/specs/` is empty, so there are no existing requirements to modify.

## Impact

**Deleted**: `ROADMAP.md` (100 KB), `CHANGELOG.md`.

**Rewritten**: `README.md`, `CONTRIBUTING.md`, `.agents/skills/commit/SKILL.md`.

**Reference fixes** (10 files): `docs/architecture.md`, `docs/domain.md`,
`docs/event-sourcing.md`, `docs/grpc.md`,
`docs/adr/0005-orchestration-vs-choreography.md`, `tools/iot-simulator/README.md`,
`tools/iot-simulator/src/plantkeeper/iot_simulator/scenarios.py` (docstring only),
`tests/integration/test_sagas.py` (docstring only), plus the two files above.

**Added**: `openspec/specs/**` (baseline specs), `.github/` templates.

**No runtime behavior changes.** The two source-file edits are docstrings; no
aggregate, handler, consumer, projection, schema, contract or migration changes. No new
dependencies. No CI: `.github/` gains templates only, matching the project's rule that
CI/CD is out of scope.

**Explicitly out of scope**: `.docs/` (gitignored working notes), and any notion of
preserving the roadmap's per-task history — it stays in git history.
