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

> **Статус:** ✅ выполнено — 7 bounded contexts и 21 доменное событие в
> `packages/domain`, `make lint && make test` зелёные, `make test-domain` держит
> покрытие домена на 100% (порог 90%). Коммиты созданы локально, push в `origin`
> не выполнялся.

### 1.1. Общие Value Objects
- [x] `HouseholdId`, `PlantId`, `SensorId`, `SpeciesId`, `JournalEntryId`, `NotificationId`, `UserId` (UUID-обёртки) · `S` 🧪
- [x] `Location` (строка с валидацией длины 1–100) · `S` 🧪
- [x] `Moisture` (float 0–100, immutable) · `S` 🧪
- [x] `Temperature` (−50..+60 °C) · `S` 🧪
- [x] `LightLevel` (люкс, ≥ 0) · `S` 🧪
- [x] `WateringInterval`, `FertilizingInterval`, `RepottingInterval` (timedelta > 0) · `S` 🧪
- [x] `CareRule` (полив, удобрение, пересадка) · `M` 🧪
- [x] `SensorReading` (sensor_id, recorded_at, moisture, temp, light) · `S` 🧪
- [x] Коммит: `feat(domain): value objects` · `M` 🧪

**DoD:** все VO покрыты тестами на валидацию границ.

### 1.2. Базовые классы домена
- [x] `Entity` (базовый класс с `id`) · `S`
- [x] `AggregateRoot` (наследует Entity, содержит `_events`, `collect_events()`) · `M` 🧪
- [x] `DomainEvent` (Pydantic BaseModel с `event_id`, `occurred_at`) · `S` 🧪
- [x] `DomainError` (базовое исключение) · `S`
- [x] Коммит: `feat(domain): base classes for entities and events` · `M` 🧪

### 1.3. Bounded Context: Garden
- [x] Агрегат `Plant` (id, household_id, species_id, name, location, added_at) · `M` 🧪
- [x] Инварианты: name не пустое; нельзя полить дважды за час; нельзя пересадить чаще 6 мес · `M` 🧪
- [x] Команды: `add_plant`, `remove_plant`, `move_plant`, `water` · `M` 🧪
- [x] События: `PlantAdded`, `PlantRemoved`, `PlantMoved`, `PlantOnboarded` · `M` 🧪
- [x] Агрегат `Household` (max 50 растений) · `M` 🧪
- [x] Коммит: `feat(domain): garden context` · `M` 🧪

### 1.4. Bounded Context: Care
- [x] Агрегат `CareSchedule` (plant_id, watering_interval, next_watering_at, version) · `M` 🧪
- [x] Инварианты: interval > 0; next ≥ now; version инкрементируется · `M` 🧪
- [x] Оптимистичная блокировка через `version` · `S` 🧪
- [x] События: `CareScheduleCreated`, `WateringDue`, `WateringCompleted`, `WateringRescheduled`, `CareMissed`, `CareSkipped` · `M` 🧪
- [x] Коммит: `feat(domain): care context` · `M` 🧪

### 1.5. Bounded Context: Catalog
- [x] Агрегат `Species` (scientific_name, common_name, watering_interval, light_requirement, version) · `M` 🧪
- [x] События: `SpeciesSyncRequested`, `SpeciesUpdated`, `SpeciesCacheInvalidated` · `S` 🧪
- [x] Коммит: `feat(domain): catalog context` · `M` 🧪

### 1.6. Bounded Context: Journal (Event Sourcing)
- [x] Агрегат `JournalEntry` (append-only, immutable) · `M` 🧪
- [x] `JournalStream` — упорядоченный список записей по plant_id · `M` 🧪
- [x] Событие `JournalEntryAdded` · `S` 🧪
- [x] Коммит: `feat(domain): journal context (event-sourced)` · `M` 🧪

### 1.7. Bounded Context: Telemetry
- [x] Агрегат `Sensor` (sensor_id, plant_id, added_at) · `S` 🧪
- [x] События: `TelemetryReceived`, `SoilMoistureLow`, `SoilMoistureHigh`, `TemperatureAnomaly`, `SensorOffline` · `M` 🧪
- [x] Коммит: `feat(domain): telemetry context` · `M` 🧪

### 1.8. Bounded Context: Notifications
- [x] Агрегат `Notification` (household_id, type, payload, created_at, read_at) · `M` 🧪
- [x] События: `NotificationCreated`, `NotificationRead` · `S` 🧪
- [x] Коммит: `feat(domain): notifications context` · `M` 🧪

### 1.9. Bounded Context: Identity
- [x] Агрегат `User` (без auth, просто участник household) · `S` 🧪
- [x] Коммит: `feat(domain): identity context` · `S` 🧪

### 1.10. Документация домена
- [x] `docs/domain.md` — Ubiquitous Language глоссарий · `M` 📝
- [x] `docs/domain.md` — диаграмма агрегатов (Mermaid) · `M` 📝
- [x] `docs/events.md` — каталог 21 доменного события · `M` 📝
- [x] ADR `0002-bounded-contexts.md` — почему 8 BC · `M` 📝
- [x] Коммит: `docs: domain glossary + BC map` · `M` 📝

### 1.11. Контракт независимости контекстов
- [x] import-linter contract `independence` для 7 контекстов домена · `S` 🧪
- [x] Коммит: `chore(lint): enforce domain context independence` · `S`

### 1.12. Контрактный тест каталога событий
- [x] `tests/unit/domain/test_events_catalogue.py` — 21 событие, базовые поля, JSON round-trip · `S` 🧪
- [x] Коммит: `test(domain): event catalogue contract` · `S` 🧪

**✅ Phase 1 завершена, когда:** покрытие domain ≥ 90%, все инварианты покрыты тестами, `docs/domain.md` и `docs/events.md` заполнены.

---

## Phase 2 — Write Side (REST + Outbox) ✅

> **Цель:** работающий write path: HTTP-команда → агрегат → БД → outbox → Kafka.
> **Результат фазы:** `POST /api/v1/plants` создаёт растение и публикует `PlantAdded`.

### 2.1. Порты (Repository, UoW, Outbox, EventPublisher)
- [x] `PlantRepository` (Protocol): `add`, `get`, `save`, `delete`, `list_by_household` · `M`
- [x] `HouseholdRepository`, `CareScheduleRepository`, `SpeciesRepository`, `SensorRepository`, `JournalEntryRepository`, `NotificationRepository`; `get_for_update` добавляют household, care schedule, species и notification · `M`
- [x] `UnitOfWork` (Protocol): репозитории, `commit`, `rollback`, `__aenter__`, `__aexit__` · `M`
- [x] `OutboxRepository` (Protocol): `append`, `fetch_unpublished`, `mark_published`, `record_failure`, `dead_letter` + `OutboxMessage` · `M`
- [x] `IdempotencyRepository` (Protocol) и порт `Clock` · `S`
- [x] `EventPublisher` (Protocol): `publish`, `publish_dead_letter` · `S`
- [x] Коммит: `feat(application): repository, uow, outbox and publisher ports` · `M`

### 2.2. Persistence: SQLAlchemy 2.0 async
- [x] `Base` (DeclarativeBase) в `infrastructure/persistence/` с naming convention · `S`
- [x] ORM-модель `PlantModel` (таблица `plants` в схеме `write_garden`) · `M`
- [x] ORM-модели для Care, Catalog, Journal, Telemetry, Notifications и общей `write_shared` · `L` → разбито на 6 задач · `M` каждая
- [x] Маппинг ORM ↔ домен (mapper-функции по контекстам + outbox) · `M` 🧪
- [x] `SqlAlchemy*Repository` для каждого порта (9 реализаций, по одной на порт) · `L` 🧪
- [x] `SqlAlchemyUnitOfWork` (session factory, commit, rollback, `IntegrityError` → `ConcurrentWriteError`) · `M` 🧪
- [x] `AggregateTracker` — какие агрегаты записали события, в порядке записи · `S` 🧪
- [x] Коммит: `feat(infra): sqlalchemy models, mappers and repositories` · `L` 🧪

### 2.3. Alembic миграции
- [x] `alembic init` + async-конфиг (`env.py` строит URL из `Settings`) · `M`
- [x] Миграция `0001_write_schema`: 8 схем, 9 таблиц, индексы (включая частичный по outbox) · `M` 🧪
- [x] `make migrate` применяет всё с нуля; `upgrade` → `downgrade base` → `upgrade` проверены тестом · `S` 🧪
- [x] `docker/postgres/init/01-schemas.sql` дополнен схемой `write_shared` · `S`
- [x] Коммит: `feat(infra): alembic migrations for write schema` · `M` 🧪

### 2.4. Transactional Outbox
- [x] Таблица `write_shared.outbox` (`event_id` unique, `topic`, `partition_key`, `payload` JSONB, `occurred_at`, `created_at`, `published_at`, `dead_lettered_at`, `attempts`, `last_error`) · `S`
- [x] Частичный индекс `ix_outbox_unpublished` по `id WHERE published_at IS NULL AND dead_lettered_at IS NULL` · `S`
- [x] `SqlAlchemyOutboxRepository` (append, fetch_unpublished, mark_published, record_failure, dead_letter) · `M` 🧪
- [x] Outbox в `SqlAlchemyUnitOfWork.commit()`: агрегаты и события — одна транзакция · `M` 🧪
- [x] Тест: команда + событие в outbox — атомарно, и провал транзакции не оставляет ни агрегата, ни события · `M` 🧪
- [x] Таблица `write_shared.idempotency_keys` + репозиторий · `S` 🧪
- [x] Коммит: `feat(infra): transactional outbox` · `M` 🧪

### 2.5. Outbox Relay (publisher в Kafka)
- [x] `OutboxRelay` — фоновый цикл со своей сессией, батч, коммит на сообщение · `M` 🧪
- [x] Retry с exponential backoff (tenacity) внутри одного опроса · `S` 🧪
- [x] DLQ `plantkeeper.dlq.v1` после `outbox_max_attempts` (5) неудачных опросов; неудачная копия в DLQ оставляет строку на повтор · `M` 🧪
- [x] `attempts` считает неудачные опросы, поэтому рестарт не сбрасывает счётчик · `S` 🧪
- [x] Интеграция Relay в lifecycle воркера (`make workers`, graceful shutdown по SIGINT/SIGTERM); тестом покрыт `OutboxRelay.run`/`stop`, сам `apps/workers/main.py` — нет · `M`
- [x] Коммит: `feat(infra): faststream kafka publisher and outbox relay` · `M` 🧪

### 2.6. Kafka producer (FastStream)
- [x] `KafkaBroker` singleton + конфиг из env (`KAFKA_BOOTSTRAP_SERVERS`) · `S`
- [x] `KafkaEventPublisher` (реализация EventPublisher) · `M` 🧪
- [x] Один топик на контекст (`garden.events`, `care.events`, …), `event_name`/`event_id` в headers, ключ из `partition_key_for` · `M` 🧪
- [x] Топики создаются автоматически (auto-create) · `S`
- [x] Коммит: `feat(infra): faststream kafka publisher` · `M` 🧪

### 2.7. DI (Dishka)
- [x] `AppProvider` (settings через pydantic-settings, clock, engine, session_factory, request map) · `M`
- [x] `DatabaseProvider` (session, UoW) · `M`
- [x] `MessagingProvider` (KafkaBroker, publisher, relay) · `M`
- [x] `RepositoryProvider` + `build_handler_provider()` (18 хендлеров, REQUEST scope) · `M` 🧪
- [x] Интеграция Dishka с FastAPI (`setup_dishka`, `FromDishka`, `DishkaCQRSContainer`) · `M` 📝
- [x] Коммит: `feat(infra): dishka providers and the outbox relay worker` · `M` 🧪

### 2.8. Application слой: команды и запросы
- [x] `Command` / `Query` базовые классы (pydantic) + `IdempotentCommand` · `S`
- [x] `CommandHandler` / `QueryHandler` базовые классы · `S`
- [x] `CreateHouseholdCommand` + handler · `S`
- [x] `AddPlantCommand` + handler (создаёт агрегат, блокирует household, сохраняет) · `M`
- [x] `RemovePlantCommand`, `MovePlantCommand` + handlers · `S`
- [x] `WaterPlantCommand`, `SkipWateringCommand` + handlers (проверяют инварианты) · `M`
- [x] `RequestSpeciesSync`, `AddSensor`, `RemoveSensor`, `AcknowledgeNotification` + handlers · `M`
- [x] Запросы: растения, household, care today, species (list/get), sensors, pending notifications · `M`
- [x] `views.py` — Pydantic-проекции и `CollectionView`; `registry.py` — `RequestMap` на 18 запросов · `M`
- [x] `idempotency.py` — `request_fingerprint` и `commit_create` (replay, конфликт, гонка) · `M`
- [x] Коммит: `feat(application): use cases for garden, care, catalog, telemetry and notifications` · `L`

### 2.9. REST API (FastAPI) — Garden
- [x] `main.py` с `FastAPI()`, DI, lifespan, exception handlers (409/404/422) · `M`
- [x] Router `/api/v1/plants`: POST, GET, GET/{id}, PATCH (move), DELETE · `M` 🧪
- [x] Router `/api/v1/households`: POST, GET/{id} — нужен, чтобы создавать растения · `M` 🧪
- [x] Pydantic-схемы (`PlantCreate`, `PlantUpdate`, `PlantResponse`, …) и `Idempotency-Key` · `M`
- [x] Маппинг HTTP ↔ Command/Query через mediator · `M`
- [x] OpenAPI-документация доступна на `/docs` · `S` 🧪
- [x] Коммит: `feat(api): write side rest endpoints` · `L` 🧪

### 2.10. REST API — Care, Catalog, Sensors, Notifications
- [x] Router `/api/v1/care`: GET today, POST water, POST skip · `M` 🧪
- [x] Router `/api/v1/sensors`: POST, GET, DELETE · `M` 🧪
- [x] Router `/api/v1/catalog`: GET species, GET/{id}, POST sync (202 + `SpeciesSyncRequested` в outbox) · `M` 🧪
- [x] Router `/api/v1/notifications`: GET pending, POST ack · `M` 🧪
- [x] Коммит: `feat(api): write side rest endpoints` (роутеры вышли одним коммитом с 2.9: `build_api_router` импортирует их все) · `L` 🧪

### 2.11. E2E-тест write side
- [x] Тест: `POST /plants` → 201, растение в БД, событие в outbox · `M` 🧪
- [x] Тест: `POST /plants` → outbox relay → событие в Kafka (testcontainers) · `M` 🧪
- [x] Тест: повторный `POST /plants` с тем же idempotency key не дублирует; другой body с тем же ключом — 409 · `M` 🧪
- [x] Тесты: инварианты через HTTP (404/422/409), OpenAPI, sensors, catalog sync · `M` 🧪
- [x] Коммит: `test(e2e): write side flow` · `M` 🧪

### 2.12. Документация фазы
- [x] ADR `0003-write-side-outbox.md` — почему outbox свой, а не из `python-cqrs` (сверено с исходниками `python-cqrs` 4.13) · `M` 📝
- [x] `docs/events.md` — раздел Transport: топики, body, headers, ключ, at-least-once, DLQ · `M` 📝
- [x] `CHANGELOG.md` — записи фазы · `S` 📝
- [x] Коммит: `docs: outbox adr, event transport and changelog` · `M` 📝

**✅ Phase 2 завершена, когда:** `POST /api/v1/plants` работает end-to-end, событие в Kafka, e2e-тест зелёный.

**Статус:** ✅ завершена 2026-09-20. `make lint` чист, `make test` — 332 теста (unit + integration + e2e на testcontainers), покрытие 99%.

**Отклонения и уточнения:**
- `RepositoryProvider` отдаёт порты репозиториев только для чтения: write-хендлеры берут репозитории из `UnitOfWork`, чтобы сессия и транзакция были одними и теми же. Сами порты от этого не изменились.
- Отдельных unit-тестов на команды и запросы нет: путь покрыт интеграционными (`test_repositories.py` — репозитории, UoW, outbox, идемпотентность) и e2e-тестами (HTTP → outbox → Kafka), а `tests/unit/infrastructure/` — топиками, relay, мапперами и DI. Все 18 операций из OpenAPI-документа вызываются хотя бы одним тестом.
- `apps/workers/main.py` (signal handlers, `run`, `main`) не покрыт тестами: тестом импортируется только пакет `plantkeeper.workers` (smoke-тест), в отчёт coverage файл не попадает. Тест на lifecycle воркера уместнее в Phase 3, когда в этом же процессе появятся консьюмеры.
- Задачи, которых не было в исходном плане: зависимости (`asyncpg`, `faststream[kafka]`, `pydantic-settings`, `tenacity`, `alembic`), `POST /api/v1/households`, таблица `write_shared.idempotency_keys`, `make api` / `make workers`, ADR 0003.
- `POST /api/v1/catalog/sync` кладёт `SpeciesSyncRequested` в outbox напрямую; сага, которая его обработает, — Phase 4.
- Значения value objects (`Location`, `Moisture`, …) уходят в JSON как скаляры (`"location": "Shelf"`), а не как `{"value": "Shelf"}`: контракт события не должен выдавать устройство домена (`fix(domain): serialise scalar value objects as their scalar`).
- ADR последующих фаз переномерованы на +1 (см. отклонения Phase 3): `0005-orchestration-vs-choreography` (Phase 4), `0006-rest-and-grpc` (Phase 7), `0007-why-python-cqrs`, `0008-outbox-pattern`, `0009-event-sourcing-journal` (Phase 10).

---

## Phase 3 — CQRS Read Side + Django Admin 📖

> **Цель:** проекции событий в read-схему, Django Admin как UI для чтения.
> **Результат фазы:** созданное через REST растение видно в Django Admin.

> **Статус:** ✅ выполнено — `apps/admin` читает события и пишет `read_analytics`,
> `make lint && make test` зелёные (398 тестов, покрытие 98%), `make migrate`
> применяет Alembic-схему и Django-схему, `make admin` поднимает read side на
> :8001. Коммиты созданы локально, push в `origin` не выполнялся.

### 3.1. Django setup (ASGI)
- [x] `apps/admin/src/plantkeeper/admin/settings.py` (БД — read-схема, INSTALLED_APPS) · `M`
- [x] `manage.py` в `apps/admin/` · `S`
- [x] `asgi.py`: Starlette + Mount Django + FastStream lifespan · `M` 📝
- [x] `uv run python apps/admin/manage.py migrate` создаёт read-схему · `S`
- [x] Коммит: `feat(admin): asgi django setup` · `M`

### 3.2. Read-модели (проекции)
- [x] Read-модель `PlantReadModel` (id, name, species_name, location, next_watering) · `M`
- [x] Read-модель `CareReadModel` · `M`
- [x] Read-модель `NotificationReadModel` · `M`
- [x] Read-модель `JournalReadModel` · `M`
- [x] Django-миграции для read-схемы · `M`
- [x] Коммит: `feat(admin): read models` · `L`

### 3.3. Проекции (consumer'ы)
- [x] `PlantProjection` — слушает `PlantAdded`, `PlantRemoved`, `PlantOnboarded` · `M` 🧪
- [x] `CareProjection` — слушает `CareScheduleCreated`, `WateringCompleted`, `WateringRescheduled` · `M` 🧪
- [x] `NotificationProjection` — слушает `NotificationCreated` · `S` 🧪
- [x] `JournalProjection` — слушает `JournalEntryAdded` · `M` 🧪
- [x] Идемпотентность каждой проекции (по `event_id`) · `M` 🧪
- [x] Коммит: `feat(admin): projections` · `L`

### 3.4. Django Admin UI
- [x] `PlantAdmin` (list_display: name, species, location, next_watering) · `M`
- [x] `CareAdmin`, `NotificationAdmin`, `JournalAdmin` · `M`
- [x] Фильтры и поиск · `S`
- [x] Inline-отображение журнала на странице растения · `M`
- [x] Коммит: `feat(admin): django admin ui` · `M`

### 3.5. E2E-тест CQRS
- [x] Тест: `POST /plants` → проекция → растение в read-модели · `M` 🧪
- [x] Тест: проекция идемпотентна (повторное событие не дублирует) · `M` 🧪
- [x] Коммит: `test(e2e): cqrs projection` · `M` 🧪

**✅ Phase 3 завершена, когда:** write → Kafka → projection → read-модель работает, растение видно в Django Admin.

**Отклонения и уточнения:**
- Задача 3.3 названа `feat(workers): projections`, но проекции живут в `apps/admin` и коммит называется `feat(admin): projections`. Причины: только в `apps/admin` есть Django, Starlette и FastStream одновременно; ROADMAP 3.1 требует FastStream lifespan именно в admin-процессе; контракт import-linter держит `plantkeeper.workers` и `plantkeeper.admin` независимыми, а воркер не имеет Django, чтобы писать read-модели. `apps/workers` остаётся relay'ем write-side. См. `docs/adr/0004-read-side-projections.md`.
- `make migrate` теперь применяет обе схемы: Alembic (`write_*`) и Django (`read_analytics`). Схема read-side создаётся Django-миграцией, а не Alembic: владелец схемы — Django.
- Добавлено сверх плана: `SpeciesReadModel` + `SpeciesProjection` (источник `plants.species_name`), обработка `PlantMoved` (иначе локация в админке устаревает), `CareSkipped`/`CareMissed` (иначе расписание устаревает после skip) и `NotificationRead` (иначе `read_at` не обновляется после ack); `EVENT_TYPES` в `topics.py`; таблица `read_analytics.processed_events`; middleware `DevAutoLoginMiddleware`; `conftest`-бутстрап Django на импорте.
- Read-модель `plants` собирается из трёх топиков (garden, care, catalog), поэтому её garden-колонки nullable: событие care/journal может быть спроецировано раньше `PlantAdded`. Правило «один писатель на колонку» сохранено, а `update_or_create` не затирает чужие колонки.
- В Phase 3 нет продюсеров у `CareScheduleCreated` (Phase 4), `JournalEntryAdded` (Phase 6) и `SpeciesUpdated` (Phase 9), поэтому эти проекции покрыты интеграционными тестами с прямыми событиями; e2e-путь проверяется на `PlantAdded`. Список непроецируемых событий зафиксирован тестом `test_the_projected_catalogue_is_accounted_for`.
- `create_admin_application` — фабрика без модульного `application`: `uvicorn --factory`. Так тесты могут направить read side на свои контейнеры, а модуль не конфигурирует Django при импорте.
- Тест `tests/e2e/test_write_side.py` теперь выбирает Kafka-сообщение по `event_id`: read-side e2e-тесты публикуют в тот же топик, и «первое `PlantAdded` в топике» перестало быть сообщением этого теста.
- Зависимости: `psycopg[binary]` (бэкенд Django для Postgres) и `asgiref` (прямой `sync_to_async`) в `apps/admin`; mypy-оверрайды для `django.*` и `plantkeeper.admin.*`; исключение RUF012 для Django-моделей и миграций.
- ADR последующих фаз переномерованы ещё на +1: `0005-orchestration-vs-choreography` (Phase 4), `0006-rest-and-grpc` (Phase 7), `0007-why-python-cqrs`, `0008-outbox-pattern`, `0009-event-sourcing-journal` (Phase 10).

---

## Phase 4 — Sagas 🎭

> **Цель:** 4 саги с компенсациями.
> **Результат фазы:** онбординг растения запускает цепочку, адаптивный полив реагирует на телеметрию.

> **Статус:** ✅ выполнено — `OnboardPlantSaga` и `SpeciesSyncSaga` работают на
> движке саг `python-cqrs`, `AdaptiveWateringSaga` и `MissedCareSaga` — как
> choreography-consumer'ы в `apps/workers`; `make workers` поднимает relay,
> консьюмеров саг и их таймеры. `make lint && make test` зелёные (452 теста,
> покрытие 97%), `make migrate` применяет Alembic `0002`. Коммиты созданы локально,
> push в `origin` не выполнялся.

### 4.1. Инфраструктура саг
- [x] Таблица `saga_state` (saga_id, type, state, step, created_at, updated_at) · `M`
- [x] `SagaStateRepository` · `M` 🧪
- [x] Базовый класс `Saga` (orchestration): steps, compensate, `handle_event` · `M` 🧪
- [x] Интеграция с `python-cqrs` SagaMap · `M` 📝
- [x] Коммит: `feat(application): saga infrastructure` · `L` 🧪

### 4.2. OnboardPlantSaga (orchestration)
- [x] Шаг 1: получить `species_id` из Catalog (через ACL) · `M` 🧪
- [x] Шаг 2: создать `CareSchedule` (из species.watering_interval) · `M` 🧪
- [x] Шаг 3: создать первое напоминание (Notification) · `M` 🧪
- [x] Шаг 4: publish `PlantOnboarded` · `S` 🧪
- [x] Компенсация: удалить CareSchedule + Notification при ошибке · `M` 🧪
- [x] Тест: успешный путь · `M` 🧪
- [x] Тест: компенсация при ошибке в шаге 2 · `M` 🧪
- [x] Коммит: `feat(application): OnboardPlantSaga` · `L` 🧪

### 4.3. AdaptiveWateringSaga (choreography)
- [x] Consumer на `TelemetryReceived` — оценить moisture < threshold · `M` 🧪
- [x] Проверить: до планового полива > 2 дня? · `S` 🧪
- [x] Опубликовать `WateringRescheduled` · `S` 🧪
- [x] Обработка `SoilMoistureHigh` (перелив) · `M` 🧪
- [x] Тест: сухая почва → сдвиг расписания · `M` 🧪
- [x] Тест: перелив → уведомление · `M` 🧪
- [x] Коммит: `feat(application): AdaptiveWateringSaga` · `M` 🧪

### 4.4. MissedCareSaga
- [x] Cron / scheduled event `WateringDue` · `M` 🧪
- [x] Grace period 24ч (scheduled check) · `M` 🧪
- [x] Публикация `CareMissed` + Notification · `S` 🧪
- [x] Сдвиг расписания · `S` 🧪
- [x] Тест: `WateringDue` без `WateringCompleted` → `CareMissed` через 24ч (freezegun) · `M` 🧪
- [x] Коммит: `feat(application): MissedCareSaga` · `M` 🧪

### 4.5. SpeciesSyncSaga
- [x] Триггер: cron раз в сутки + ручной `POST /catalog/sync` · `M` 🧪
- [x] Fetch Trefle → diff → publish `SpeciesUpdated` для изменённых · `M` 🧪
- [x] Инвалидация Valkey-кэша · `S` 🧪
- [x] Компенсация: откат к предыдущей версии species при ошибке · `M` 🧪
- [x] Коммит: `feat(application): SpeciesSyncSaga` · `L` 🧪

### 4.6. Документация саг
- [x] `docs/sagas.md` — описание 4 саг с диаграммами (Mermaid sequence) · `L` 📝
- [x] ADR `0005-orchestration-vs-choreography.md` · `M` 📝
- [x] Коммит: `docs: sagas description` · `M` 📝

**✅ Phase 4 завершена, когда:** 4 саги покрыты тестами, `docs/sagas.md` заполнен.

**Отклонения и уточнения:**
- **Свой `ISagaStorage`.** Библиотечный `SqlAlchemySagaStorage` держит таблицы без
  схемы (`saga_executions`/`saga_logs`), берёт их имена из env **на импорте** и
  объявляет вторую declarative base. Наш адаптер реализует тот же протокол над
  `write_shared.saga_state` / `write_shared.saga_log`, которые создаёт Alembic
  `0002`. Причина и последствия — ADR 0005.
- **`saga_state` разбита на две таблицы.** Колонка `step` из роадмапа не может
  восстановить, *какие* шаги уже прошли и что компенсировать; историю шагов
  хранит `saga_log`, а `saga_state` — статус, контекст, версию и счётчик recovery.
- **Choreography — это `Consumer`, а не `Saga`.** `AdaptiveWateringSaga` и
  `MissedCareSaga` не имеют процесса-координатора, поэтому у них нет шагов и
  компенсаций, а есть реакция на событие. Таймер MissedCare — строка
  `write_care.missed_care_windows`, а не таблица `saga_state`.
- **`python-cqrs` саги не событийные.** Движок запускается по типу контекста, а не
  по событию, поэтому каждое событие-триггер сначала превращается в контекст
  (`Saga.context_from_event`); `handle_event` — наш метод, а не библиотечный.
- **Контекст саги — dataclass, а не Pydantic.** Это требование библиотеки
  (`SagaContext.to_dict/from_dict` через `dataclass_wizard`); поля контекста —
  JSON-скаляры, и в них не протекает устройство домена. Доменные события остаются
  Pydantic.
- **Базовый класс `Saga` не параметризован контекстом.** Класс наследует
  `__orig_bases__`, поэтому `Saga[SagaContext]` заставил бы валидатор библиотеки
  читать *базовый* контекст у каждой конкретной саги и отвергать её шаги. Тип
  контекста объявлен в `context_type` и по нему строится `SagaMap`.
- **Детерминированный `saga_id`** = `uuid5(<saga_name>, <correlation_id>)`: повторная
  доставка возобновляет ту же сагу, а не создаёт вторую.
- **События жизненного цикла саг** (`SagaStarted`, `SagaCompleted`, `SagaFailed`,
  `SagaCompensated`) добавлены в Phase 4 и публикуются в новый топик `saga.events`
  (каталог 21 → 25 событий). Их не проецирует ни одна read-модель.
- **Идемпотентность write-side консьюмеров** — новая таблица
  `write_shared.processed_events` с уникальностью `(consumer_group, event_id)`;
  в роадмапе 4.1 её не было, но `AGENTS.md` требует идемпотентности от каждого
  консьюмера, а `read_analytics.processed_events` принадлежит Django.
- **Джоб восстановления саг** (`SagaRecoveryJob`) добавлен сверх роадмапа: без него
  сага, упавшая в `running`, остаётся там навсегда. Он использует библиотечный
  `recover_saga` и ограничен настройками `SAGA_RECOVERY_*`.
- **`SagaStateRepository` пока без вызывающего в проде.** Recovery ходит в
  библиотечный `ISagaStorage.get_sagas_for_recovery`, который отвечает только
  идентификаторами и фильтрует по имени саги; 4.1 требует репозиторий, поэтому он
  оставлен как читающий фасад над той же таблицей (`get` + `list_recoverable`) и
  покрыт интеграционным тестом. По той же причине в реестре есть `saga_type_named`:
  `saga_state` хранит имя класса, и это единственный обратный путь к самой саге.
- **Вместо freezegun** (роадмап 4.4) тест двигает `Clock` порт: в проекте время —
  это зависимость, которую подменяет тест, а не заморозка процесса.
- **Компенсация при ошибке в шаге 2** (роадмап 4.2) проверена как ошибка в шаге 3:
  падение именно второго шага не оставляет данных и потому ничего не компенсирует;
  тест проверяет откат уже созданного расписания.
- **`SpeciesSyncSaga` не создаёт виды.** В каталоге нет события «вид создан», поэтому
  незнакомый `species_id` пропускается с предупреждением; создание — вопрос Phase 9
  вместе с Trefle. `SpeciesSource` в Phase 4 — заглушка, возвращающая пустой список;
  «инвалидация кэша» публикует `SpeciesCacheInvalidated`, а Valkey-консьюмер — Phase 9.
- **Общий декодер Kafka** (`decode_event` с header/body) переехал из
  `apps/admin` в `plantkeeper.infrastructure.messaging.decoding`: `apps/admin` и
  `apps/workers` не могут импортировать друг друга (контракт слоёв).
- **`partition_key_for`** теперь учитывает `saga_id`, иначе события одной саги
  разъехались бы по партициям.
- **`make workers` теперь и консьюмер, а не только relay**; lifecycle воркера
  (`run`, `_request_shutdown`, сборка джобов) вынесен так, чтобы его можно было
  тестировать, а stop-контракт джобов покрыт unit-тестом.
- Новых зависимостей нет: `python-cqrs` объявлен в `apps/workers` напрямую (как уже
  было в `apps/api`), остальное — существующие SQLAlchemy, Dishka, FastStream.
- ADR последующих фаз не менялись: `0006-rest-and-grpc` (Phase 7),
  `0007-why-python-cqrs`, `0008-outbox-pattern`, `0009-event-sourcing-journal` (Phase 10).

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
- [ ] ADR `0006-rest-and-grpc.md` — почему оба протокола · `M` 📝
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
- [ ] ADR `0007-why-python-cqrs.md` · `M` 📝
- [ ] ADR `0008-outbox-pattern.md` · `M` 📝
- [ ] ADR `0009-event-sourcing-journal.md` · `M` 📝
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
