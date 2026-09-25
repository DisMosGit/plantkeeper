# PlantKeeper — developer entry points.
#
# `migrate`, `api`, `grpc`, `admin`, `workers` and the `iot*` targets run the real
# processes; `docs/iot-simulator.md` describes the simulator's flags, and
# `docs/grpc.md` the gRPC server and its stub generation.

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose

.PHONY: help dev up down clean lint format test test-unit test-domain test-integration test-e2e coverage migrate api proto grpc admin admin-static workers iot iot-drought iot-dry-run contracts diagrams

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

lint: proto ## Ruff + Mypy + import-linter
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy .
	uv run lint-imports

format: ## Apply Ruff formatting and autofixes
	uv run ruff format .
	uv run ruff check --fix .

test: proto ## Run all tests with coverage
	uv run pytest --cov=plantkeeper --cov-report=term-missing

test-unit: proto ## Unit tests only
	uv run pytest tests/unit

test-domain: ## Domain unit tests with the Phase 1 coverage floor (90%)
	uv run pytest tests/unit/domain --cov=plantkeeper.domain --cov-report=term-missing --cov-fail-under=90

test-integration: ## Integration tests (needs Docker)
	uv run pytest tests/integration -m integration

test-e2e: proto ## End-to-end tests (start their own containers; needs Docker)
	uv run pytest tests/e2e -m slow

coverage: proto ## Full run, then the per-layer floors (domain 90, application 80, infrastructure 70)
	uv run pytest --cov=plantkeeper --cov-report=term-missing --cov-report=html:docs/coverage.html
	@echo "--- layers (floor: domain 90, application 80, infrastructure 70) ---"
	uv run coverage report --include="*/plantkeeper/domain/*"
	uv run coverage report --include="*/plantkeeper/application/*"
	uv run coverage report --include="*/plantkeeper/infrastructure/*"
	@echo "HTML report: docs/coverage.html"

migrate: ## Apply all migrations (Alembic write schema + Django read schema)
	uv run alembic upgrade head
	uv run python apps/admin/manage.py migrate

api: ## Run the FastAPI write API on :8000
	uv run uvicorn plantkeeper.api.main:app --host 0.0.0.0 --port 8000 --reload

proto: ## Regenerate the gRPC Python stubs from proto/ (they are not committed)
	uv run python tools/protogen.py

contracts: ## Export the OpenAPI/AsyncAPI documents and render the Mermaid diagrams
	uv run python tools/contracts.py

diagrams: ## Render docs/diagrams/*.md from the event and saga registries
	uv run python tools/contracts.py --diagrams-only

grpc: proto ## Run the gRPC write API on :50051
	uv run python -m plantkeeper.api.grpc

admin-static: ## Collect the Django Admin's static files for the Starlette mount
	uv run python apps/admin/manage.py collectstatic --noinput

admin: admin-static ## Run the read side (projections + Django Admin) on :8001
	uv run uvicorn --factory plantkeeper.admin.asgi:create_admin_application --host 0.0.0.0 --port 8001 --reload

workers: ## Run the write-side workers: outbox relay, saga consumers and their timers
	uv run python -m plantkeeper.workers

iot: ## Run the IoT simulator: 20 sensors, 10s interval, --scenario normal --seed 42
	uv run python -m plantkeeper.iot_simulator --scenario normal --seed 42

iot-drought: ## Run the IoT simulator with --scenario drought --seed 42 (trips AdaptiveWateringSaga)
	uv run python -m plantkeeper.iot_simulator --scenario drought --seed 42

iot-dry-run: ## Print the simulated stream to stdout (--dry-run), without touching Kafka
	uv run python -m plantkeeper.iot_simulator --dry-run --scenario normal --seed 42
