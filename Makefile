# PlantKeeper — developer entry points.
#
# Phase 2 fills in `migrate`, `api` and `workers`; `iot*` are explicit
# placeholders that later phases fill in (see ROADMAP.md).

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose

.PHONY: help dev up down clean lint format test test-unit test-domain test-integration test-e2e migrate api admin admin-static workers iot iot-drought

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

dev: ## Start local infra (Kafka, Postgres, Valkey, Console) and wait until healthy
	$(COMPOSE) up -d --wait --wait-timeout 300

up: ## Start local infra in the background
	$(COMPOSE) up -d

down: ## Stop local infra, keep volumes
	$(COMPOSE) down

clean: ## Stop local infra and delete volumes
	$(COMPOSE) down -v --remove-orphans

lint: ## Ruff + Mypy + import-linter
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy .
	uv run lint-imports

format: ## Apply Ruff formatting and autofixes
	uv run ruff format .
	uv run ruff check --fix .

test: ## Run all tests with coverage
	uv run pytest --cov=plantkeeper --cov-report=term-missing

test-unit: ## Unit tests only
	uv run pytest tests/unit

test-domain: ## Domain unit tests with the Phase 1 coverage floor (90%)
	uv run pytest tests/unit/domain --cov=plantkeeper.domain --cov-report=term-missing --cov-fail-under=90

test-integration: ## Integration tests (needs Docker)
	uv run pytest tests/integration -m integration

test-e2e: ## End-to-end tests (start their own containers; needs Docker)
	uv run pytest tests/e2e -m slow

migrate: ## Apply all migrations (Alembic write schema + Django read schema)
	uv run alembic upgrade head
	uv run python apps/admin/manage.py migrate

api: ## Run the FastAPI write API on :8000
	uv run uvicorn plantkeeper.api.main:app --host 0.0.0.0 --port 8000 --reload

admin-static: ## Collect the Django Admin's static files for the Starlette mount
	uv run python apps/admin/manage.py collectstatic --noinput

admin: admin-static ## Run the read side (projections + Django Admin) on :8001
	uv run uvicorn --factory plantkeeper.admin.asgi:create_admin_application --host 0.0.0.0 --port 8001 --reload

workers: ## Run the write-side workers: outbox relay, saga consumers and their timers
	uv run python -m plantkeeper.workers

iot: ## Run the IoT simulator, normal scenario (added in Phase 5)
	@echo "IoT simulator lands in Phase 5: uv run python -m plantkeeper.iot_simulator --scenario normal"

iot-drought: ## Run the IoT simulator, drought scenario (added in Phase 5)
	@echo "IoT simulator lands in Phase 5: uv run python -m plantkeeper.iot_simulator --scenario drought"
