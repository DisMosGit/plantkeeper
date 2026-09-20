# 🗺 PlantKeeper Roadmap

> Каждая задача — **атомарная**: один осмысленный коммит, чёткое Definition of Done, минимум зависимостей от незавершённых задач.

---

## 📖 Как читать этот документ

- **Фазы** выполняются последовательно. Внутри фазы задачи можно брать в любом порядке, если не указано `depends on`.
- **Атомарность** = задача завершается за один рабочий подход (30 мин – 4 часа) и оставляет репозиторий в рабочем состоянии.
- **DoD** (Definition of Done) — обязательные критерии завершения.
- **Оценка** — грубая: `S` (< 1ч), `M` (1–3ч), `L` (3–8ч). `XL` — признак, что задачу надо дробить.
- **Метка 🧪** — задача сопровождается тестами.
- **Метка 📝** — задача сопровождается документацией (README, ADR, docstring).

Статусы: `[ ]` — не начато, `[~]` — в работе, `[x]` — готово, `[-]` — отменено.

---

## 🎯 Целевое состояние (Definition of Done проекта)

- ✅ Монорепо на uv workspace со всеми пакетами и приложениями.
- ✅ REST API (FastAPI) + gRPC + Django Admin + FastStream workers работают одновременно.
- ✅ Kafka в KRaft-режиме, Postgres, Valkey поднимаются одной командой `docker compose up -d`.
- ✅ DDD-домен с 8 bounded contexts, агрегатами, VO, инвариантами.
- ✅ CQRS: раздельные write/read схемы.
- ✅ 4 саги (2 orchestration, 2 choreography) с компенсациями.
- ✅ Transactional Outbox + Idempotent Consumer.
- ✅ Event Sourcing для Journal.
- ✅ IoT-симулятор с физической моделью и 5 сценариями.
- ✅ HTTP long polling для уведомлений.
- ✅ Trefle ACL с Circuit Breaker.
- ✅ AsyncAPI + OpenAPI + Protobuf контракты.
- ✅ Покрытие тестами: domain ≥ 90%, application ≥ 80%, infrastructure ≥ 70%.
- ✅ `make lint && make test` проходят локально.

---

## Phase 0 — Skeleton 🦴

> **Статус:** ✅ выполнено — подзадачи 0.1–0.8 закрыты, `make lint && make test`
> зелёные, `make dev` поднимает инфру (все сервисы `healthy`). Коммиты созданы локально,
> push в `origin` не выполнялся.

> **Цель:** пустой репозиторий превращается в монорепо, где всё линтится, тестируется и поднимается.
> **Результат фазы:** `make dev` поднимает инфру, `make test` проходит (0 тестов — тоже результат).

### 0.1. Инициализация репозитория
- [x] `git init`, создать `.gitignore` (Python, uv, IDE, `.env`, `*.pyc`) · `S`
- [x] Создать `LICENSE.md` (MIT) · `S` 📝
- [x] Создать `README.md` (обзор + ссылки на docs) · `S` 📝
- [x] Создать `CONTRIBUTING.md` · `S` 📝
- [x] Создать `CHANGELOG.md` (Keep a Changelog) · `S` 📝
- [x] Первый коммит: `chore: initial skeleton` · `S`

**DoD фазы 0.1:** репозиторий создан, четыре корневых `.md` на месте, первый коммит запушен.

### 0.2. uv workspace
- [x] Создать корневой `pyproject.toml` с `[tool.uv.workspace]` · `S` 📝
- [x] Создать директории `packages/domain`, `packages/application`, `packages/infrastructure` с пустыми `src/` · `S`
- [x] Создать директории `apps/api`, `apps/admin`, `apps/workers` · `S`
- [x] Создать директорию `tools/iot-simulator` · `S`
- [x] Создать директории `proto/`, `tests/`, `docs/`, `docs/adr/` · `S`
- [x] `uv sync --all-packages` работает без ошибок · `S`
- [x] Коммит: `chore: uv workspace skeleton` · `S`

**DoD фазы 0.2:** структура директорий на месте, `uv sync` проходит.

### 0.3. Инфраструктура (docker-compose)
- [x] `docker-compose.yml` с Kafka в KRaft-режиме (без Zookeeper) · `M`
- [x] Добавить Postgres 18 с init-скриптом для схем `write_*` и `read_*` · `M`
- [x] Добавить Valkey 9 · `S`
- [x] Добавить Redpanda Console (Kafka UI) на `:8080` · `S`
- [x] Healthchecks для всех сервисов · `M`
- [x] `docker compose up -d` поднимает всё без ошибок · `S`
- [x] Коммит: `chore(infra): docker-compose with Kafka KRaft, Postgres, Valkey` · `M`

**DoD фазы 0.3:** `docker compose up -d` — все контейнеры `healthy`.

### 0.4. Тулинг (Ruff, Mypy, pytest, pre-commit)
- [x] Конфиг `ruff` в корневом `pyproject.toml` (line-length, target-version, select) · `S`
- [x] Конфиг `mypy` strict в корневом `pyproject.toml` · `S`
- [x] Конфиг `pytest` + `pytest-asyncio` (asyncio_mode = "auto") · `S`
- [x] Конфиг `coverage` (branch, fail_under) · `S`
- [x] `.pre-commit-config.yaml`: ruff, ruff-format, mypy, check-yaml, end-of-file-fixer · `M`
- [x] `uv run pre-commit install` работает · `S`
- [x] `uv run pre-commit run --all-files` проходит · `S`
- [x] Коммит: `chore: ruff + mypy + pytest + pre-commit` · `M`

**DoD фазы 0.4:** `pre-commit run --all-files` — все хуки passed.

### 0.5. Makefile / justfile
- [x] `make dev` — поднять инфру + запустить сервисы · `M`
- [x] `make lint` — ruff check + ruff format --check + mypy · `S`
- [x] `make format` — ruff format + ruff check --fix · `S`
- [x] `make test` / `make test-unit` / `make test-integration` / `make test-e2e` · `M`
- [x] `make migrate` — alembic + django migrate · `S`
- [x] `make iot` / `make iot-drought` — запуск IoT-симулятора · `S`
- [x] `make clean` — stop + rm volumes · `S`
- [x] Коммит: `chore: Makefile with dev/lint/test/migrate/iot/clean` · `M`

**DoD фазы 0.5:** `make lint` и `make test` проходят на пустом проекте.

### 0.6. Каркасы `pyproject.toml` для пакетов
- [x] `packages/domain/pyproject.toml` (зависимости: только pydantic) · `S`
- [x] `packages/application/pyproject.toml` (depends on domain + python-cqrs) · `S`
- [x] `packages/infrastructure/pyproject.toml` (SQLAlchemy, aiokafka, valkey, httpx, dishka) · `S`
- [x] `apps/api/pyproject.toml` (fastapi, grpcio, uvicorn) · `S`
- [x] `apps/admin/pyproject.toml` (django, starlette, faststream[kafka]) · `S`
- [x] `apps/workers/pyproject.toml` (faststream[kafka], dishka) · `S`
- [x] `tools/iot-simulator/pyproject.toml` (aiokafka, stdlib) · `S`
- [x] `uv sync --all-packages` проходит · `M`
- [x] `import-linter` настроен: domain ← application ← infrastructure ← apps · `M` 📝
- [x] Коммит: `chore: package pyproject.toml + import-linter` · `M`

**DoD фазы 0.6:** `uv sync --all-packages` + `lint-imports` (import-linter) проходит.

### 0.7. Первые пустые тесты
- [x] `tests/conftest.py` с базовыми фикстурами (`event_loop`, `anyio_backend`) · `S`
- [x] `tests/unit/test_smoke.py` — `assert True` (проверка, что pytest работает) · `S` 🧪
- [x] `tests/integration/__init__.py` + заготовка testcontainers fixture · `M`
- [x] `tests/e2e/__init__.py` · `S`
- [x] Коммит: `test: pytest scaffolding` · `M` 🧪

### 0.8. Документация фазы 0
- [x] `docs/architecture.md` — пустой шаблон с заголовками · `S` 📝
- [x] `docs/domain.md` — пустой шаблон с заголовками · `S` 📝
- [x] `docs/adr/0001-record-architecture-decisions.md` — стартовый ADR · `S` 📝
- [x] `docs/adr/template.md` — шаблон ADR · `S` 📝
- [x] `ROADMAP.md` (этот документ) · `M` 📝
- [x] Коммит: `docs: architecture, domain, ADR templates, roadmap` · `M`

**✅ Phase 0 завершена, когда:** `make dev` поднимает инфру, `make lint && make test` проходят, все директории и pyproject.toml на месте.

---

## Phase 1 — Domain (DDD) 🧠

> **Цель:** чистый домен без зависимостей от инфраструктуры.
> **Результат фазы:** агрегаты, VO, события, инварианты с покрытием ≥ 90%.

### 1.1. Общие Value Objects
- [ ] `HouseholdId`, `PlantId`, `SensorId`, `SpeciesId`, `JournalEntryId` (UUID-обёртки) · `S` 🧪
- [ ] `Location` (строка с валидацией длины 1–100) · `S` 🧪
- [ ] `Moisture` (float 0–100, immutable) · `S` 🧪
- [ ] `Temperature` (−50..+60 °C) · `S` 🧪
- [ ] `LightLevel` (люкс, ≥ 0) · `S` 🧪
- [ ] `WateringInterval` (timedelta > 0) · `S` 🧪
- [ ] `CareRule` (полив, удобрение, пересадка) · `M` 🧪
- [ ] `SensorReading` (sensor_id, recorded_at, moisture, temp, light) · `S` 🧪
- [ ] Коммит: `feat(domain): value objects` · `M` 🧪

**DoD:** все VO покрыты тестами на валидацию границ.

### 1.2. Базовые классы домена
- [ ] `Entity` (базовый класс с `id`) · `S`
- [ ] `AggregateRoot` (наследует Entity, содержит `_events`, `collect_events()`) · `M` 🧪
- [ ] `DomainEvent` (Pydantic BaseModel с `event_id`, `occurred_at`) · `S` 🧪
- [ ] `DomainError` (базовое исключение) · `S`
- [ ] Коммит: `feat(domain): base classes for entities and events` · `M` 🧪

### 1.3. Bounded Context: Garden
- [ ] Агрегат `Plant` (id, household_id, species_id, name, location, added_at) · `M` 🧪
- [ ] Инварианты: name не пустое; нельзя полить дважды за час; нельзя пересадить чаще 6 мес · `M` 🧪
- [ ] Команды: `add_plant`, `remove_plant`, `move_plant`, `water` · `M` 🧪
- [ ] События: `PlantAdded`, `PlantRemoved`, `PlantMoved`, `PlantOnboarded` · `M` 🧪
- [ ] Агрегат `Household` (max 50 растений) · `M` 🧪
- [ ] Коммит: `feat(domain): garden context` · `M` 🧪

### 1.4. Bounded Context: Care
- [ ] Агрегат `CareSchedule` (plant_id, watering_interval, next_watering_at, version) · `M` 🧪
- [ ] Инварианты: interval > 0; next ≥ now; version инкрементируется · `M` 🧪
- [ ] Оптимистичная блокировка через `version` · `S` 🧪
- [ ] События: `CareScheduleCreated`, `WateringDue`, `WateringCompleted`, `WateringRescheduled`, `CareMissed`, `CareSkipped` · `M` 🧪
- [ ] Коммит: `feat(domain): care context` · `M` 🧪

### 1.5. Bounded Context: Catalog
- [ ] Агрегат `Species` (scientific_name, common_name, watering_interval, light_requirement, version) · `M` 🧪
- [ ] События: `SpeciesSyncRequested`, `SpeciesUpdated`, `SpeciesCacheInvalidated` · `S` 🧪
- [ ] Коммит: `feat(domain): catalog context` · `M` 🧪

### 1.6. Bounded Context: Journal (Event Sourcing)
- [ ] Агрегат `JournalEntry` (append-only, immutable) · `M` 🧪
- [ ] `JournalStream` — упорядоченный список записей по plant_id · `M` 🧪
- [ ] Событие `JournalEntryAdded` · `S` 🧪
- [ ] Коммит: `feat(domain): journal context (event-sourced)` · `M` 🧪

### 1.7. Bounded Context: Telemetry
- [ ] Агрегат `Sensor` (sensor_id, plant_id, added_at) · `S` 🧪
- [ ] События: `TelemetryReceived`, `SoilMoistureLow`, `SoilMoistureHigh`, `TemperatureAnomaly`, `SensorOffline` · `M` 🧪
- [ ] Коммит: `feat(domain): telemetry context` · `M` 🧪

### 1.8. Bounded Context: Notifications
- [ ] Агрегат `Notification` (household_id, type, payload, created_at, read_at) · `M` 🧪
- [ ] События: `NotificationCreated`, `NotificationRead` · `S` 🧪
- [ ] Коммит: `feat(domain): notifications context` · `M` 🧪

### 1.9. Bounded Context: Identity
- [ ] Агрегат `User` (без auth, просто участник household) · `S` 🧪
- [ ] Коммит: `feat(domain): identity context` · `S` 🧪

### 1.10. Документация домена
- [ ] `docs/domain.md` — Ubiquitous Language глоссарий · `M` 📝
- [ ] `docs/domain.md` — диаграмма агрегатов (Mermaid) · `M` 📝
- [ ] ADR `0002-bounded-contexts.md` — почему 8 BC · `M` 📝
- [ ] Коммит: `docs: domain glossary + BC map` · `M` 📝

**✅ Phase 1 завершена, когда:** покрытие domain ≥ 90%, все инварианты покрыты тестами, `docs/domain.md` заполнен.

---

## Phase 2 — Write Side (REST + Outbox) ✍️

> **Цель:** работающий write path: HTTP-команда → агрегат → БД → событие в Kafka.
> **Результат фазы:** `POST /api/v1/plants` создаёт растение и публикует `PlantAdded`.

### 2.1. Порты (Repository, UoW, EventPublisher)
- [ ] `PlantRepository` (Protocol): `add`, `get`, `save`, `delete` · `M`
- [ ] `CareScheduleRepository`, `SpeciesRepository`, `SensorRepository`, `NotificationRepository` · `M`
- [ ] `UnitOfWork` (Protocol): `commit`, `rollback`, `__aenter__`, `__aexit__` · `M`
- [ ] `EventPublisher` (Protocol): `publish` · `S`
- [ ] Коммит: `feat(application): repository and uow ports` · `M`

### 2.2. Persistence: SQLAlchemy 2.0 async
- [ ] `Base` (DeclarativeBase) в `infrastructure/persistence/` · `S`
- [ ] ORM-модель `PlantModel` (таблица `plants` в схеме `write_garden`) · `M`
- [ ] ORM-модели для Care, Catalog, Journal, Telemetry, Notifications · `L` → разбить на 5 задач · `M` каждая
- [ ] Маппинг ORM ↔ домен (mapper-функции) · `M` 🧪
- [ ] `SqlAlchemyPlantRepository` (реализация порта) · `M` 🧪
- [ ] `SqlAlchemyUnitOfWork` (session factory, commit, rollback) · `M` 🧪
- [ ] Коммит: `feat(infra): sqlalchemy models + repositories` · `L`

### 2.3. Alembic миграции
- [ ] `alembic init` + конфиг с async engine · `M`
- [ ] Миграция для `write_garden.plants` · `S`
- [ ] Миграции для остальных таблиц write-схемы · `M`
- [ ] `make migrate` применяет всё с нуля · `S`
- [ ] Коммит: `feat(infra): alembic migrations for write schema` · `M`

### 2.4. Transactional Outbox
- [ ] Таблица `outbox` (event_id, topic, payload, created_at, published_at, attempts) · `S`
- [ ] `OutboxRepository` (append, fetch_unpublished, mark_published) · `M` 🧪
- [ ] Интеграция Outbox в `SqlAlchemyUnitOfWork.commit()` (одна транзакция) · `M` 🧪
- [ ] Тест: команда + событие в outbox — атомарно · `M` 🧪
- [ ] Коммит: `feat(infra): transactional outbox` · `M` 🧪

### 2.5. Outbox Relay (publisher в Kafka)
- [ ] `OutboxRelay` — фоновый цикл, читает unpublished, публикует в Kafka · `M` 🧪
- [ ] Retry с exponential backoff (tenacity) · `S` 🧪
- [ ] DLQ после 5 попыток · `M` 🧪
- [ ] Интеграция Relay в FastStream worker lifecycle · `M` 🧪
- [ ] Коммит: `feat(infra): outbox relay to kafka` · `M` 🧪

### 2.6. Kafka producer (FastStream)
- [ ] `KafkaBroker` singleton + конфиг из env (`KAFKA_BOOTSTRAP_SERVERS`) · `S`
- [ ] `KafkaEventPublisher` (реализация EventPublisher) · `M` 🧪
- [ ] Топики создаются автоматически (auto-create) · `S`
- [ ] Коммит: `feat(infra): faststream kafka publisher` · `M`

### 2.7. DI (Dishka)
- [ ] `AppProvider` (config, settings через pydantic-settings) · `M`
- [ ] `DatabaseProvider` (engine, session_factory, UoW) · `M`
- [ ] `MessagingProvider` (KafkaBroker, publisher) · `M`
- [ ] `RepositoryProvider` (все репозитории) · `M`
- [ ] Интеграция Dishka с FastAPI (`setup_dishka`, `FromDishka`) · `M` 📝
- [ ] Коммит: `feat(infra): dishka providers` · `M`

### 2.8. Application слой: команды и запросы
- [ ] `Command` / `Query` базовые классы (pydantic) · `S`
- [ ] `CommandHandler` / `QueryHandler` базовые классы · `S`
- [ ] `AddPlantCommand` + `AddPlantHandler` (создаёт агрегат, сохраняет, публикует) · `M` 🧪
- [ ] `RemovePlantCommand` + handler · `S` 🧪
- [ ] `MovePlantCommand` + handler · `S` 🧪
- [ ] `WaterPlantCommand` + handler (проверяет инвариант) · `M` 🧪
- [ ] `AddSensorCommand` + handler · `S` 🧪
- [ ] `GetPlantQuery` + handler · `S` 🧪
- [ ] `ListPlantsQuery` + handler · `S` 🧪
- [ ] Коммит: `feat(application): garden commands and queries` · `L`

### 2.9. REST API (FastAPI) — Garden
- [ ] `main.py` с `FastAPI()`, DI, exception handlers · `M`
- [ ] Router `/api/v1/plants`: POST, GET, GET/{id}, PATCH, DELETE · `M` 🧪
- [ ] Pydantic-схемы: `PlantCreate`, `PlantUpdate`, `PlantResponse` · `M`
- [ ] Маппинг HTTP ↔ Command/Query · `M`
- [ ] OpenAPI-документация доступна на `/docs` · `S`
- [ ] Коммит: `feat(api): garden REST endpoints` · `L`

### 2.10. REST API — Care, Catalog, Sensors
- [ ] Router `/api/v1/care`: GET today, POST water, POST skip · `M` 🧪
- [ ] Router `/api/v1/sensors`: POST, GET, DELETE · `M` 🧪
- [ ] Router `/api/v1/catalog`: GET species, POST sync · `M` 🧪
- [ ] Router `/api/v1/notifications`: GET pending, POST ack · `M` 🧪
- [ ] Коммит: `feat(api): care, sensors, catalog, notifications endpoints` · `L`

### 2.11. E2E-тест write side
- [ ] Тест: `POST /plants` → 201, растение в БД, событие в outbox · `M` 🧪
- [ ] Тест: `POST /plants` → outbox relay → событие в Kafka (testcontainers) · `M` 🧪
- [ ] Тест: повторный `POST /plants` с тем же idempotency key — не дублирует · `M` 🧪
- [ ] Коммит: `test(e2e): write side flow` · `M` 🧪

**✅ Phase 2 завершена, когда:** `POST /api/v1/plants` работает end-to-end, событие в Kafka, e2e-тест зелёный.

---

## Phase 3 — CQRS Read Side + Django Admin 📖

> **Цель:** проекции событий в read-схему, Django Admin как UI для чтения.
> **Результат фазы:** созданное через REST растение видно в Django Admin.

### 3.1. Django setup (ASGI)
- [ ] `apps/admin/src/plantkeeper/admin/settings.py` (БД — read-схема, INSTALLED_APPS) · `M`
- [ ] `manage.py` в `apps/admin/` · `S`
- [ ] `asgi.py`: Starlette + Mount Django + FastStream lifespan · `M` 📝
- [ ] `uv run python apps/admin/manage.py migrate` создаёт read-схему · `S`
- [ ] Коммит: `feat(admin): asgi django setup` · `M`

### 3.2. Read-модели (проекции)
- [ ] Read-модель `PlantReadModel` (id, name, species_name, location, next_watering) · `M`
- [ ] Read-модель `CareReadModel` · `M`
- [ ] Read-модель `NotificationReadModel` · `M`
- [ ] Read-модель `JournalReadModel` · `M`
- [ ] Django-миграции для read-схемы · `M`
- [ ] Коммит: `feat(admin): read models` · `L`

### 3.3. Проекции (consumer'ы)
- [ ] `PlantProjection` — слушает `PlantAdded`, `PlantRemoved`, `PlantOnboarded` · `M` 🧪
- [ ] `CareProjection` — слушает `CareScheduleCreated`, `WateringCompleted`, `WateringRescheduled` · `M` 🧪
- [ ] `NotificationProjection` — слушает `NotificationCreated` · `S` 🧪
- [ ] `JournalProjection` — слушает `JournalEntryAdded` · `M` 🧪
- [ ] Идемпотентность каждой проекции (по `event_id`) · `M` 🧪
- [ ] Коммит: `feat(workers): projections` · `L`

### 3.4. Django Admin UI
- [ ] `PlantAdmin` (list_display: name, species, location, next_watering) · `M`
- [ ] `CareAdmin`, `NotificationAdmin`, `JournalAdmin` · `M`
- [ ] Фильтры и поиск · `S`
- [ ] Inline-отображение журнала на странице растения · `M`
- [ ] Коммит: `feat(admin): django admin ui` · `M`

### 3.5. E2E-тест CQRS
- [ ] Тест: `POST /plants` → проекция → растение в read-модели · `M` 🧪
- [ ] Тест: проекция идемпотентна (повторное событие не дублирует) · `M` 🧪
- [ ] Коммит: `test(e2e): cqrs projection` · `M` 🧪

**✅ Phase 3 завершена, когда:** write → Kafka → projection → read-модель работает, растение видно в Django Admin.

---

## Phase 4 — Sagas 🎭

> **Цель:** 4 саги с компенсациями.
> **Результат фазы:** онбординг растения запускает цепочку, адаптивный полив реагирует на телеметрию.

### 4.1. Инфраструктура саг
- [ ] Таблица `saga_state` (saga_id, type, state, step, created_at, updated_at) · `M`
- [ ] `SagaStateRepository` · `M` 🧪
- [ ] Базовый класс `Saga` (orchestration): steps, compensate, `handle_event` · `M` 🧪
- [ ] Интеграция с `python-cqrs` SagaMap · `M` 📝
- [ ] Коммит: `feat(application): saga infrastructure` · `L` 🧪

### 4.2. OnboardPlantSaga (orchestration)
- [ ] Шаг 1: получить `species_id` из Catalog (через ACL) · `M` 🧪
- [ ] Шаг 2: создать `CareSchedule` (из species.watering_interval) · `M` 🧪
- [ ] Шаг 3: создать первое напоминание (Notification) · `M` 🧪
- [ ] Шаг 4: publish `PlantOnboarded` · `S` 🧪
- [ ] Компенсация: удалить CareSchedule + Notification при ошибке · `M` 🧪
- [ ] Тест: успешный путь · `M` 🧪
- [ ] Тест: компенсация при ошибке в шаге 2 · `M` 🧪
- [ ] Коммит: `feat(application): OnboardPlantSaga` · `L` 🧪

### 4.3. AdaptiveWateringSaga (choreography)
- [ ] Consumer на `TelemetryReceived` — оценить moisture < threshold · `M` 🧪
- [ ] Проверить: до планового полива > 2 дня? · `S` 🧪
- [ ] Опубликовать `WateringRescheduled` · `S` 🧪
- [ ] Обработка `SoilMoistureHigh` (перелив) · `M` 🧪
- [ ] Тест: сухая почва → сдвиг расписания · `M` 🧪
- [ ] Тест: перелив → уведомление · `M` 🧪
- [ ] Коммит: `feat(application): AdaptiveWateringSaga` · `M` 🧪

### 4.4. MissedCareSaga
- [ ] Cron / scheduled event `WateringDue` · `M` 🧪
- [ ] Grace period 24ч (scheduled check) · `M` 🧪
- [ ] Публикация `CareMissed` + Notification · `S` 🧪
- [ ] Сдвиг расписания · `S` 🧪
- [ ] Тест: `WateringDue` без `WateringCompleted` → `CareMissed` через 24ч (freezegun) · `M` 🧪
- [ ] Коммит: `feat(application): MissedCareSaga` · `M` 🧪

### 4.5. SpeciesSyncSaga
- [ ] Триггер: cron раз в сутки + ручной `POST /catalog/sync` · `M` 🧪
- [ ] Fetch Trefle → diff → publish `SpeciesUpdated` для изменённых · `M` 🧪
- [ ] Инвалидация Valkey-кэша · `S` 🧪
- [ ] Компенсация: откат к предыдущей версии species при ошибке · `M` 🧪
- [ ] Коммит: `feat(application): SpeciesSyncSaga` · `L` 🧪

### 4.6. Документация саг
- [ ] `docs/sagas.md` — описание 4 саг с диаграммами (Mermaid sequence) · `L` 📝
- [ ] ADR `0003-orchestration-vs-choreography.md` · `M` 📝
- [ ] Коммит: `docs: sagas description` · `M` 📝

**✅ Phase 4 завершена, когда:** 4 саги покрыты тестами, `docs/sagas.md` заполнен.

---

## Phase 5 — IoT Simulator 🌡

> **Цель:** реалистичный поток телеметрии в Kafka.
> **Результат фазы:** `make iot` генерирует данные 20 сенсоров, они попадают в care-consumers.

### 5.1. Физическая модель
- [ ] `SensorState` (moisture, temperature, light, last_watered_at) · `M` 🧪
- [ ] Диуральный цикл освещённости (синусоида от времени суток) · `M` 🧪
- [ ] Модель высыхания почвы (экспонента, зависит от temp + light) · `M` 🧪
- [ ] Гауссов шум + редкие выбросы · `S` 🧪
- [ ] Коммит: `feat(iot): physical model` · `M` 🧪

### 5.2. Сценарии
- [ ] `normal` — базовая модель · `S`
- [ ] `drought` — ускоренное высыхание · `S` 🧪
- [ ] `overwatering` — moisture > 90% · `S` 🧪
- [ ] `cold_snap` — температура падает · `S` 🧪
- [ ] `sensor_failure` — сенсор замолкает на N минут · `S` 🧪
- [ ] Коммит: `feat(iot): scenarios` · `M` 🧪

### 5.3. Publisher в Kafka
- [ ] `aiokafka` producer в топик `telemetry.raw` · `M` 🧪
- [ ] Батчинг (100 сообщений / 1 сек) · `S` 🧪
- [ ] Graceful shutdown · `S` 🧪
- [ ] Коммит: `feat(iot): kafka publisher` · `M` 🧪

### 5.4. CLI
- [ ] `--sensors N` (default 20) · `S`
- [ ] `--interval SEC` (default 10) · `S`
- [ ] `--scenario NAME` · `S`
- [ ] `--seed INT` (детерминизм) · `S`
- [ ] `--replay FILE.jsonl` · `M` 🧪
- [ ] `--dry-run` (без Kafka, в stdout) · `S`
- [ ] Коммит: `feat(iot): CLI` · `M`

### 5.5. Care consumer на телеметрию
- [ ] Consumer `telemetry.raw` → валидация → publish `TelemetryReceived` · `M` 🧪
- [ ] Дедупликация по `(sensor_id, recorded_at)` · `M` 🧪
- [ ] Batch insert в `write_telemetry.sensor_readings` (партиционирование по времени) · `M` 🧪
- [ ] Коммит: `feat(workers): telemetry consumer` · `L` 🧪

### 5.6. E2E-тест IoT
- [ ] Тест: симулятор → Kafka → care consumer → БД · `M` 🧪
- [ ] Тест: сценарий `drought` → `AdaptiveWateringSaga` сдвигает расписание · `M` 🧪
- [ ] Коммит: `test(e2e): iot telemetry flow` · `M` 🧪

**✅ Phase 5 завершена, когда:** `make iot` + `make iot-drought` работают, e2e-тесты зелёные.

---

## Phase 6 — Event Sourcing (Journal) 📜

> **Цель:** append-only Event Store для журнала ухода.
> **Результат фазы:** можно «отмотать» состояние журнала на любую дату.

### 6.1. Event Store
- [ ] Таблица `event_store` (stream_id, version, event_type, payload, occurred_at) · `M`
- [ ] `EventStoreRepository` (append, load_stream, load_all) · `M` 🧪
- [ ] Optimistic concurrency по `(stream_id, version)` · `M` 🧪
- [ ] Коммит: `feat(infra): event store` · `M` 🧪

### 6.2. Journal aggregate + handlers
- [ ] `JournalAggregate` — восстанавливается из стрима · `M` 🧪
- [ ] Команда `AddJournalEntry` + handler (append в стрим) · `M` 🧪
- [ ] Запрос `GetJournalAtDate` — восстановить состояние на дату · `M` 🧪
- [ ] Запрос `GetJournalTimeline` — все записи · `S` 🧪
- [ ] Коммит: `feat(application): journal event sourcing` · `M` 🧪

### 6.3. Snapshots
- [ ] Таблица `journal_snapshots` (stream_id, version, state) · `M`
- [ ] Автоматический snapshot каждые N событий · `M` 🧪
- [ ] `load_stream` использует snapshot если есть · `M` 🧪
- [ ] Коммит: `feat(infra): journal snapshots` · `M` 🧪

### 6.4. REST + Admin для журнала
- [ ] `GET /api/v1/journal/{plant_id}` · `M` 🧪
- [ ] `GET /api/v1/journal/{plant_id}/at?date=...` · `M` 🧪
- [ ] Django Admin: timeline журнала на странице растения · `M`
- [ ] Коммит: `feat(api+admin): journal endpoints` · `M`

**✅ Phase 6 завершена, когда:** журнал append-only, можно получить состояние на любую дату.

---

## Phase 7 — gRPC 📡

> **Цель:** gRPC API для Care на том же application-слое.
> **Результат фазы:** gRPC-клиент может вызвать `GetTodayCare`.

### 7.1. Protobuf definitions
- [ ] `proto/plantkeeper/v1/care.proto` (CareService: GetTodayCare, CompleteWatering) · `M`
- [ ] `proto/plantkeeper/v1/garden.proto` (GardenService: ListPlants, GetPlant) · `M`
- [ ] `proto/plantkeeper/v1/common.proto` (общие типы: PlantId, SensorReading) · `M`
- [ ] `buf` конфиг или `grpcio-tools` Make-таргет · `M`
- [ ] Генерация Python-стабов в `apps/api/src/.../grpc/generated/` · `M`
- [ ] Коммит: `feat(proto): care and garden services` · `M`

### 7.2. gRPC server
- [ ] `CareServicer` (grpc.aio) — вызывает QueryBus/CommandBus · `M` 🧪
- [ ] `GardenServicer` · `M` 🧪
- [ ] `server.py` — точка входа, DI, graceful shutdown · `M`
- [ ] Health-check (`grpc.health.v1.Health`) · `S` 🧪
- [ ] Reflection (для grpcurl) · `S`
- [ ] Коммит: `feat(api): grpc server` · `L` 🧪

### 7.3. gRPC-клиент для тестов
- [ ] Async gRPC client stub в `tests/` · `M` 🧪
- [ ] E2E-тест: gRPC `GetTodayCare` → правильный ответ · `M` 🧪
- [ ] Коммит: `test(e2e): grpc care service` · `M` 🧪

### 7.4. Документация
- [ ] `docs/grpc.md` — как запустить, как вызвать через grpcurl · `M` 📝
- [ ] ADR `0004-rest-and-grpc.md` — почему оба протокола · `M` 📝
- [ ] Коммит: `docs: grpc` · `M` 📝

**✅ Phase 7 завершена, когда:** `grpcurl` из README работает, e2e-тест зелёный.

---

## Phase 8 — Notifications (HTTP long polling) 🔔

> **Цель:** клиент получает уведомления через long polling.
> **Результат фазы:** `GET /notifications/pending?timeout=30` висит до появления уведомления.

### 8.1. Valkey Pub/Sub канал
- [ ] `NotificationChannel` (publish/subscribe через asyncio) · `M` 🧪
- [ ] При создании `Notification` — publish в канал `household:{id}` · `M` 🧪
- [ ] Коммит: `feat(infra): notification channel` · `M` 🧪

### 8.2. Long polling endpoint
- [ ] `GET /api/v1/notifications/pending?household_id=...&timeout=30` · `M` 🧪
- [ ] Если есть непрочитанные — вернуть сразу · `S` 🧪
- [ ] Если нет — subscribe Valkey, ждать с таймаутом · `M` 🧪
- [ ] `POST /api/v1/notifications/{id}/ack` · `S` 🧪
- [ ] Коммит: `feat(api): long polling notifications` · `M` 🧪

### 8.3. Notification consumer
- [ ] Слушает `WateringDue`, `CareMissed`, `WateringRescheduled`, `SoilMoistureLow` · `M` 🧪
- [ ] Создаёт `Notification` + publish в Valkey · `M` 🧪
- [ ] Idempotency по event_id · `S` 🧪
- [ ] Коммит: `feat(workers): notification consumer` · `M` 🧪

### 8.4. E2E-тест
- [ ] Тест: событие → Notification в БД → long poll получает · `M` 🧪
- [ ] Тест: таймаут → 204 No Content · `S` 🧪
- [ ] Коммит: `test(e2e): long polling` · `M` 🧪

**✅ Phase 8 завершена, когда:** клиент получает уведомления без email/Telegram.

---

## Phase 9 — Trefle ACL 🌐

> **Цель:** реальная синхронизация каталога с внешним API.
> **Результат фазы:** SpeciesSyncSaga тянет данные из Trefle, Circuit Breaker защищает от падений.

### 9.1. Trefle client (Anti-Corruption Layer)
- [ ] `TrefleClient` (httpx.AsyncClient) · `M` 🧪
- [ ] Модели ответов Trefle (Pydantic) · `M`
- [ ] Маппинг Trefle → домен `Species` · `M` 🧪
- [ ] Rate limiting (120 req/min) через `aiolimiter` · `M` 🧪
- [ ] Коммит: `feat(infra): trefle acl` · `L` 🧪

### 9.2. Circuit Breaker + Retry
- [ ] Circuit Breaker (tenacity / pybreaker) на TrefleClient · `M` 🧪
- [ ] Retry с exponential backoff · `S` 🧪
- [ ] Fallback: вернуть кэш из Valkey · `M` 🧪
- [ ] Коммит: `feat(infra): circuit breaker for trefle` · `M` 🧪

### 9.3. Valkey cache
- [ ] `SpeciesCache` (get/set/invalidate) с TTL 24ч · `M` 🧪
- [ ] Кэш-хит в GetSpeciesQuery · `S` 🧪
- [ ] Инвалидация при `SpeciesUpdated` · `S` 🧪
- [ ] Коммит: `feat(infra): species cache` · `M` 🧪

### 9.4. Финальная интеграция SpeciesSyncSaga
- [ ] Связать SpeciesSyncSaga с TrefleClient · `M` 🧪
- [ ] Синхронизация 30 видов end-to-end · `M` 🧪
- [ ] Коммит: `feat(application): species sync with trefle` · `M` 🧪

**✅ Phase 9 завершена, когда:** `POST /catalog/sync` подтягивает виды из Trefle, кэш работает.

---

## Phase 10 — Polish ✨

> **Цель:** репозиторий готов к показу в портфолио.
> **Результат фазы:** AsyncAPI, OpenAPI, диаграммы, ADR, документация.

### 10.1. Контракты
- [ ] AsyncAPI-экспорт из FastStream (`docs/asyncapi.json`) · `M`
- [ ] OpenAPI snapshot в `docs/openapi.json` · `S`
- [ ] Генерация Mermaid-диаграмм из python-cqrs · `M`
- [ ] Коммит: `docs: asyncapi + openapi contracts` · `M` 📝

### 10.2. Документация
- [ ] `docs/architecture.md` — полный BC map + sequence diagrams · `L` 📝
- [ ] `docs/patterns.md` — таблица паттернов с ссылками на код · `M` 📝
- [ ] ADR `0005-why-python-cqrs.md` · `M` 📝
- [ ] ADR `0006-outbox-pattern.md` · `M` 📝
- [ ] ADR `0007-event-sourcing-journal.md` · `M` 📝
- [ ] Обновить `README.md` финальной диаграммой · `M` 📝
- [ ] Коммит: `docs: architecture, patterns, ADRs` · `L` 📝

### 10.3. Тестовое покрытие
- [ ] Coverage report: domain ≥ 90% · `M` 🧪
- [ ] Coverage report: application ≥ 80% · `M` 🧪
- [ ] Coverage report: infrastructure ≥ 70% · `M` 🧪
- [ ] Отчёт в `docs/coverage.html` (или бейдж) · `S`
- [ ] Коммит: `test: coverage report` · `M`

### 10.4. Финальная вычитка
- [ ] Пройтись по всем `TODO` в коде — закрыть или завести issues · `M`
- [ ] Проверить, что нет `print()`, `pdb.set_trace()`, закомментированного кода · `S`
- [ ] `make lint && make test` — зелёные · `S`
- [ ] Обновить `CHANGELOG.md` релизом `v0.1.0` · `S`
- [ ] Тег `v0.1.0` · `S`
- [ ] Коммит: `chore: release v0.1.0` · `S`

**✅ Phase 10 завершена, когда:** README рендерит полную диаграмму, все ADR на месте, `v0.1.0` тегирован.

---

## 📊 Сводка по фазам

| Фаза | Тема | Задач (примерно) | Оценка |
|------|------|------------------|--------|
| 0 | Skeleton | 40 | 2–3 дня |
| 1 | Domain (DDD) | 35 | 3–4 дня |
| 2 | Write Side (REST + Outbox) | 50 | 5–7 дней |
| 3 | CQRS Read + Django Admin | 20 | 3–4 дня |
| 4 | Sagas | 30 | 4–5 дней |
| 5 | IoT Simulator | 25 | 3–4 дня |
| 6 | Event Sourcing | 15 | 2–3 дня |
| 7 | gRPC | 15 | 2–3 дня |
| 8 | Notifications | 12 | 2 дня |
| 9 | Trefle ACL | 12 | 2 дня |
| 10 | Polish | 20 | 2–3 дня |
| **Всего** | | **~270** | **~30–40 дней** |

---

## 🎯 Milestones (для GitHub Milestones)

| Milestone | Фазы | Что демонстрирует |
|-----------|------|-------------------|
| **M0 — Foundation** | 0 | Репо, tooling, инфра |
| **M1 — Domain** | 1 | Чистый DDD-домен |
| **M2 — Write Path** | 2 | REST + Outbox + Kafka |
| **M3 — Read Path** | 3 | CQRS + Django Admin |
| **M4 — Distributed** | 4 | Саги с компенсациями |
| **M5 — IoT** | 5 | Поток телеметрии |
| **M6 — Event Sourcing** | 6 | Append-only журнал |
| **M7 — Polyglot API** | 7 | REST + gRPC |
| **M8 — Real-time UX** | 8 | Long polling |
| **M9 — External** | 9 | Trefle + Circuit Breaker |
| **M10 — v0.1.0** | 10 | Портфолио-ready |

---

## 📌 Правила работы с роадмапом

1. **Атомарность важнее скорости.** Если задача кажется `XL` — дели на подзадачи прямо в этом файле.
2. **Каждая задача — коммит.** Имя коммита = тип + scope + subject (см. `CONTRIBUTING.md`).
3. **DoD обязателен.** Не закрывай задачу, если DoD не выполнен полностью.
4. **Метки 🧪 и 📝 — не опциональны.** Тесты и документация идут вместе с кодом, а не «потом».
5. **Фазы последовательны, задачи внутри фазы — параллельны.** Если задача блокирует другую, это указано в тексте.
6. **Обновляй этот файл по ходу.** Если появилась новая задача — добавь её в нужную фазу, а не в бэклог.
7. **Отменённые задачи** помечай `[-]` и оставляй с комментарием «почему».
8. **Прогресс видно в GitHub Projects.** Заведи Board с колонками: `Backlog` / `In Progress` / `Review` / `Done` и свяжи с этим файлом.
