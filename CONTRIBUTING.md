# Contributing

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

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.

Scopes: `garden`, `care`, `catalog`, `journal`, `telemetry`, `notifications`, `analytics`, `identity`, `api`, `admin`, `workers`, `iot`, `infra`, `deps`.

Examples:

```
feat(care): add AdaptiveWateringSaga
fix(telemetry): deduplicate by (sensor_id, recorded_at)
docs(adr): add ADR-003 choosing python-cqrs
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
make dev        # infra + all services
make lint       # ruff + mypy strict
make test       # all tests
make migrate    # all migrations
make iot        # IoT simulator (normal)
make clean      # stop infra, drop volumes
```

## Pull requests

1. Rebase on `main`.
2. Run `make lint && make test`.
3. Update `CHANGELOG.md` under `[Unreleased]` if user-facing.
4. Add an ADR in `docs/adr/` for architectural decisions.
5. One logical change per PR. Squash-merge.

Requires 1 approval. All comments resolved.

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
| Contract | `tests/contract/` | — |

No `time.sleep()` — use `freezegun` or `anyio`.
