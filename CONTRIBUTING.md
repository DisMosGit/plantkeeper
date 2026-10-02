# Contributing

A small, single-maintainer project, so this guide is short on purpose: how a change is
proposed and landed, how commits are written, and how to run the thing.

## How a change happens

Every change follows the same four steps.

1. **Propose.** Write the plan before the code. An OpenSpec change carries four
   artifacts: `proposal.md` (why, and what changes), `specs/**/spec.md` (the
   requirements, written as deltas), `design.md` (the decisions behind them) and
   `tasks.md` (the checklist). With an agent this is `/openspec-propose`; by hand:

   ```bash
   openspec new change "<name>"          # scaffold the change
   openspec status --change "<name>"     # which artifacts exist, and which are ready
   openspec validate "<name>" --strict   # the artifacts must pass before you start
   ```

2. **Review.** Read the artifacts before implementing anything. The proposal should be
   checkable against the repository, and each requirement should describe something a
   client or an operator can observe. A requirement that cannot be traced to a document
   or to the code is not ready.

3. **Implement.** Work through `tasks.md` one task at a time
   (`/openspec-apply-change`). Tick a task in the same commit as the work it describes,
   and never tick a task ahead of the work.

4. **Archive.** Once every task is done and the gates pass,
   `openspec archive "<name>"` merges the change's spec deltas into `openspec/specs/`
   and moves the change under `openspec/changes/archive/`.

`openspec/specs/` is never edited by hand: the archive step is what puts deltas there,
so every main spec can be traced back to the change that produced it.

The working rules that keep this honest: one atomic task is one commit, tests and
documentation ride with the code rather than following it, and a task is done only once
its stated verification has actually been run.

## Branches

`main` is protected. Use short-lived branches:

- `feat/<name>` — new feature
- `fix/<name>` — bug fix
- `chore/<name>` — tooling, deps
- `docs/<name>` — documentation
- `refactor/<name>` — refactoring
- `test/<name>` — tests only

## Commits

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `chore`, `revert`.

The subject is a single lowercase imperative line with no trailing period. Keep it
short — the longest subject in this history is 83 characters, so treat that as a
ceiling rather than a budget.

**Write a body.** Nearly every commit here carries one — the earliest scaffolding
commits are the only exceptions — saying what changed and, more usefully, why it had
to change that way, wrapped near 80 columns. They run from roughly fifteen to three
hundred words, with a median near a hundred. A subject-only commit is the exception,
not the house style.

No `Co-Authored-By` or tool-attribution trailers. Never rewrite a commit that may have
been published.

### Scopes

There is exactly one scope list and it lives here — the commit skill points at this
section rather than repeating it, so the two cannot drift apart. Pick the narrowest
scope that is true:

| Group | Scopes |
|-------|--------|
| Bounded contexts | `garden`, `care`, `catalog`, `journal`, `telemetry`, `notifications`, `analytics`, `identity` |
| Layers | `domain`, `application`, `infrastructure` |
| Applications and tools | `api`, `admin`, `workers`, `iot` |
| Cross-cutting | `contracts`, `proto`, `deps`, `docs`, `adr`, `openspec`, `infra`, `repo` |

`infra` is the tooling scope (`Makefile`, `docker-compose.yml`, `.env.example`);
`infrastructure` names the package. Changes that touch only tests or only linting use
`e2e`, `integration` or `lint`.

Examples:

```
feat(care): add AdaptiveWateringSaga
fix(telemetry): deduplicate by (sensor_id, recorded_at)
docs(adr): add ADR-0003 write-side outbox
```

## Local development

Requirements: Python 3.14+, [uv](https://docs.astral.sh/uv/), Docker + Compose v2.

```bash
uv sync --all-packages
docker compose up -d
uv run alembic upgrade head
uv run python apps/admin/manage.py migrate
uv run pre-commit install
```

Common commands:

```bash
make dev        # infra only (Kafka, both Postgres instances, Valkey, Console) — processes are separate
make lint       # ruff + mypy strict + import-linter
make test       # all tests
make migrate    # all migrations (Alembic write, Django read_analytics + read_telemetry)
make iot        # IoT simulator (normal)
make clean      # stop infra, drop volumes
```

## Pull requests

1. The work belongs to an OpenSpec change and `openspec validate "<name>" --strict`
   passes for it.
2. Rebase on `main`.
3. Run `make lint && make test`.
4. Add an ADR in `docs/adr/` for architectural decisions.
5. One logical change per PR. Squash-merge.

Requires 1 approval. All comments resolved.

## Releases

Release history lives in the repository's
[GitHub Releases](https://github.com/DisMosGit/plantkeeper/releases), not in a file
under version control. Recording it is a release-time act: tag the version and write
the notes on the release, so the tag is the record. No step of an ordinary change asks
you to write a history entry anywhere.

## Code style

- Ruff (lint + format), Mypy strict, full type hints.
- Async everywhere; no blocking calls in the event loop.
- Pydantic v2 for DTOs and events.
- Dishka for DI; no global singletons.
- No `print()` — use `structlog`.

Import rules (enforced by `import-linter`):

```
domain         ← depends on nothing
application    ← depends on domain
infrastructure ← depends on domain + application
apps/*         ← depends on all
tools/*        ← independent
```

## Tests

| Level | Path | Marker |
|-------|------|--------|
| Unit | `tests/unit/` | — |
| Integration | `tests/integration/` | `@pytest.mark.integration` |
| E2E | `tests/e2e/` | `@pytest.mark.slow` |
| Contract | `tests/unit/contracts/` | — |

No `time.sleep()` — use `freezegun` or `anyio`.
