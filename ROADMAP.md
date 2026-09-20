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
- ✅ Покрытие тестами: domain ≥ 90% (100%), application ≥ 80% (97%), infrastructure ≥ 70% (96%).
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

> **Статус:** ✅ выполнено — `tools/iot-simulator` считает физическую модель,
> проигрывает 5 сценариев и публикует в `telemetry.raw`; `TelemetryIngestConsumer`
> в `apps/workers` валидирует сырое сообщение, находит `plant_id` по реестру
> сенсоров, пишет чтение в `write_telemetry.sensor_readings` и кладёт
> `TelemetryReceived` в outbox — одной транзакцией. `make lint && make test` зелёные
> (564 теста, покрытие 97%), `make migrate` применяет Alembic `0003`, `make iot` /
> `make iot-drought` работают. Коммиты созданы локально, push в `origin` не выполнялся.

### 5.1. Физическая модель
- [x] `SensorState` (moisture, temperature, light, last_watered_at) · `M` 🧪
- [x] Диуральный цикл освещённости (синусоида от времени суток) · `M` 🧪
- [x] Модель высыхания почвы (экспонента, зависит от temp + light) · `M` 🧪
- [x] Гауссов шум + редкие выбросы · `S` 🧪
- [x] Коммит: `feat(iot): physical model` · `M` 🧪

### 5.2. Сценарии
- [x] `normal` — базовая модель · `S`
- [x] `drought` — ускоренное высыхание · `S` 🧪
- [x] `overwatering` — moisture > 90% · `S` 🧪
- [x] `cold_snap` — температура падает · `S` 🧪
- [x] `sensor_failure` — сенсор замолкает на N минут · `S` 🧪
- [x] Коммит: `feat(iot): scenarios` · `M` 🧪

### 5.3. Publisher в Kafka
- [x] `aiokafka` producer в топик `telemetry.raw` · `M` 🧪
- [x] Батчинг (100 сообщений / 1 сек) · `S` 🧪
- [x] Graceful shutdown · `S` 🧪
- [x] Коммит: `feat(iot): kafka publisher` · `M` 🧪

### 5.4. CLI
- [x] `--sensors N` (default 20) · `S`
- [x] `--interval SEC` (default 10) · `S`
- [x] `--scenario NAME` · `S`
- [x] `--seed INT` (детерминизм) · `S`
- [x] `--replay FILE.jsonl` · `M` 🧪
- [x] `--dry-run` (без Kafka, в stdout) · `S`
- [x] Коммит: `feat(iot): CLI` · `M`

### 5.5. Care consumer на телеметрию
- [x] Consumer `telemetry.raw` → валидация → publish `TelemetryReceived` · `M` 🧪
- [x] Дедупликация по `(sensor_id, recorded_at)` · `M` 🧪
- [x] Batch insert в `write_telemetry.sensor_readings` (партиционирование по времени) · `M` 🧪
- [x] Коммит: `feat(workers): telemetry consumer` · `L` 🧪

### 5.6. E2E-тест IoT
- [x] Тест: симулятор → Kafka → care consumer → БД · `M` 🧪
- [x] Тест: сценарий `drought` → `AdaptiveWateringSaga` сдвигает расписание · `M` 🧪
- [x] Коммит: `test(e2e): iot telemetry flow` · `M` 🧪

**✅ Phase 5 завершена, когда:** `make iot` + `make iot-drought` работают, e2e-тесты зелёные.

**Отклонения и уточнения:**
- **`telemetry.raw` — не топик доменных событий.** Сырое измерение не имеет
  `plant_id` и не является фактом ни одного контекста, поэтому симулятор публикует
  его напрямую, а не через outbox. `TelemetryReceived` — единственное событие
  каталога без агрегата: ingress добавляет его в outbox явно, в одной транзакции с
  чтением, и дальше его публикует relay. `TELEMETRY_RAW` не входит в `EVENT_TOPICS`,
  и это зафиксировано тестом.
- **Дедупликация — ключ таблицы, а не второй ledger.** Первичный ключ
  `(sensor_id, recorded_at)` одновременно является требованием партиционирования
  (уникальный ключ партиционированной таблицы обязан содержать ключ партиции) и
  ключом идемпотентности: `INSERT … ON CONFLICT DO NOTHING`. Отдельный ledger не
  нужен и был бы вторым местом, где claim может разойтись с записью. Событие
  `TelemetryReceived` добавляется в outbox только тогда, когда вставка действительно
  вставила строку: повторная доставка не пишет чтение и ничего не объявляет, иначе
  второй `TelemetryReceived` получил бы новый `event_id`, которого не узнал бы ни один
  consumer-group ledger.
- **`sensor_readings` действительно партиционирована по месяцам**, а не просто
  проиндексирована: `PARTITION BY RANGE (recorded_at)`, Alembic `0003` создаёт
  родителя, индексы и default-партицию, а `TelemetryPartitionJob` (новый джоб
  воркера) держит окно «текущий месяц + 3». Default-партиция — страховка: чтение с
  внеоконным `recorded_at` (replay, сбитые часы) падает туда, а не ломает запись.
- **Сенсор без регистрации отбрасывается с предупреждением.** `--sensor-base-id`
  (и необязательный `sensor_id` в `POST /api/v1/sensors`) добавлены сверх роадмапа
  именно поэтому: без них детерминированные сенсоры симулятора нельзя связать с
  растением, и критерий 5.6 «drought → сага сдвигает расписание» недостижим.
- **Ingress не использует `WORKER_CONSUMER_TYPES`.** Он читает не доменное событие,
  поэтому регистрируется отдельной функцией `register_telemetry_ingest`, а его
  consumer group (`plantkeeper-telemetry-ingest`) задаётся настройкой, а не выводится
  из имени класса. Offset reset — `latest`, не `earliest`: ledger'а для replay у
  сырой телеметрии нет, а таблица чтений и есть её запись.
- **Ingress коммитит по одному сообщению.** Батч — это паблишер (5.3), а не
  консьюмер: одна транзакция на доставку и есть то, что делает чтение и его событие
  атомарными. `add_many` при этом нарезает батч по 500 строк, если его кто-то
  позовёт пачкой.
- **Пороговые события (`SoilMoistureLow`, `SoilMoistureHigh`, `TemperatureAnomaly`,
  `SensorOffline`) по-прежнему не публикуются**: их порождает `Sensor.record`, а
  вызывающего у него пока нет — детектор появится в Phase 8 вместе с окнами и
  уведомлениями. Ingress пишет только `TelemetryReceived`.
- **`Sensor.last_seen_at` не обновляется на каждом чтении**: это была бы запись на
  каждое измерение ради вопроса, который в Phase 5 никто не задаёт; последнее
  чтение и так лежит в `sensor_readings`.
- **Вместо теста на `apps/workers/main.py`** (как и в Phase 4) lifecycle воркера
  проверяется через `register_telemetry_ingest` и сборку джобов; сам `main.py`
  по-прежнему вне покрытия.
- **`tests/integration/test_migrations.py` сравнивает модели через `pg_class`**
  (kind `r`/`p`, без партиций), а не `pg_tables`: иначе партиционированный родитель
  либо отсутствовал бы в ожидаемом множестве, либо его месячные дети выдавались бы
  за отдельные модели.
- Новых зависимостей нет: симулятор использует уже объявленный `aiokafka`, воркер —
  существующие SQLAlchemy/Dishka/FastStream.
- Нумерация ADR не менялась: `0006-rest-and-grpc` (Phase 7), `0007-why-python-cqrs`,
  `0008-outbox-pattern`, `0009-event-sourcing-journal` (Phase 10). Архитектурные
  решения Phase 5 записаны здесь и в `docs/telemetry.md`, а не отдельным ADR.

---

## Phase 6 — Event Sourcing (Journal) 📜

> **Цель:** append-only Event Store для журнала ухода.
> **Результат фазы:** можно «отмотать» состояние журнала на любую дату.

> **Статус:** ✅ выполнено — `write_journal.event_store` (Alembic `0004`) хранит по
> растению свой стрим с оптимистичной блокировкой по `(stream_id, version)`,
> `JournalAggregate` восстанавливается из стрима, снапшоты (`0005`) пишутся каждые 50
> событий, `GET /api/v1/journal/{plant_id}` и `.../at?date=` отвечают реплеем, а
> `JournalEntryConsumer` превращает `WateringCompleted` в запись журнала. `make lint`
> чист, `make test` — 623 теста (unit + integration + e2e на testcontainers),
> покрытие 97%. Коммиты созданы локально, push в `origin` не выполнялся.

### 6.1. Event Store
- [x] Таблица `event_store` (stream_id, version, event_type, payload, occurred_at) · `M`
- [x] `EventStoreRepository` (append, load_stream, load_all) · `M` 🧪
- [x] Optimistic concurrency по `(stream_id, version)` · `M` 🧪
- [x] Коммит: `feat(infra): event store` · `M` 🧪

### 6.2. Journal aggregate + handlers
- [x] `JournalAggregate` — восстанавливается из стрима · `M` 🧪
- [x] Команда `AddJournalEntry` + handler (append в стрим) · `M` 🧪
- [x] Запрос `GetJournalAtDate` — восстановить состояние на дату · `M` 🧪
- [x] Запрос `GetJournalTimeline` — все записи · `S` 🧪
- [x] Коммит: `feat(application): journal event sourcing` · `M` 🧪

### 6.3. Snapshots
- [x] Таблица `journal_snapshots` (stream_id, version, state) · `M`
- [x] Автоматический snapshot каждые N событий · `M` 🧪
- [x] `load_stream` использует snapshot если есть · `M` 🧪
- [x] Коммит: `feat(infra): journal snapshots` · `M` 🧪

### 6.4. REST + Admin для журнала
- [x] `GET /api/v1/journal/{plant_id}` · `M` 🧪
- [x] `GET /api/v1/journal/{plant_id}/at?date=...` · `M` 🧪
- [x] Django Admin: timeline журнала на странице растения · `M`
- [x] Коммит: `feat(api+admin): journal endpoints` · `M`

**✅ Phase 6 завершена, когда:** журнал append-only, можно получить состояние на любую дату.

**Отклонения и уточнения:**
- **Продюсер `JournalEntryAdded` добавлен сверх роадмапа.** Ни 6.1–6.4, ни коммиты
  фазы его не называют, но `docs/events.md` с Phase 3 назначает Journal
  потребителем `WateringCompleted`, а `docs/cqrs.md` — «до Phase 6 никто не
  публикует `JournalEntryAdded`». Без консьюмера критерий фазы («журнал
  append-only, можно получить состояние на любую дату») недостижим на работающей
  системе: журнал было бы нечем наполнить. `JournalEntryConsumer` живёт в
  `packages/application` и регистрируется в `CONSUMER_TYPES` рядом с
  choreography-сагами; `apps/workers/consumers.py` не изменился — он выводит
  группы и топики из реестра.
- **`/at?date=` фильтрует по моменту ухода (`entry_occurred_at`), а не по моменту
  записи.** Журнал — это лог ухода, и вопрос «что было сделано к этой дате»
  задаётся датой ухода; так же журнал упорядочен (`JournalStream`) и так же его
  показывает админка. Дата включительная и означает конец суток UTC (`time.max`).
  Момент записи остаётся полем события и не является осью запроса: запись,
  добавленная задним числом, всё равно попадает в свой день.
- **Идемпотентность консьюмера двойная.** Кроме ledger'а
  `(consumer_group, event_id)`, `entry_id` выводится из события
  (`watering_entry_id` = `uuid5`), и `record_journal_entry` для уже
  записанного `entry_id` — no-op. Append-only история не переживает дубликат,
  который один ledger пропустил бы при пересоздании группы (сброшенный offset +
  потерянный ledger).
- **`write_journal.journal_entries` остаётся и пишется в той же транзакции.**
  Event store — источник истины, таблица Phase 2 — синхронное табличное отражение
  тех же фактов: писатель ровно один (код append'а), разойтись они не могут,
  деструктивной миграции не нужно. Ни один read-путь её не читает; удалить её —
  отдельное решение более поздней фазы.
- **`load_stream` и снапшот.** Порт `EventStoreRepository.load_stream` читает
  события стрима после заданной версии, а «использовать снапшот, если он есть»
  (6.3) реализовано в `application/journal/store.py::load_journal_aggregate`:
  версию для чтения хвоста даёт `JournalSnapshotRepository.latest`. Так слой
  доступа к данным не знает, что такое агрегат, а политика реплея остаётся в
  application.
- **Интервал снапшотов — модульная константа** (`SNAPSHOT_EVERY = 50`), как
  `GRACE_PERIOD` и `TICK_BATCH_SIZE`, а не настройка: это политика журнала, а не
  свойство окружения. `Settings` не менялся.
- **Снапшот append-only агрегата не сжимает состояние, а ограничивает реплей.**
  Состояние журнала — это его записи, поэтому снапшот хранит их список; выигрыш в
  том, сколько строк событий десериализует реплей. Хранение и pruning истории
  снапшотов — вне области фазы (зафиксировано в `docs/event-sourcing.md`).
- **Версия и целостность.** Реплей проверяет непрерывность версий, известность
  `event_type` и то, что в стриме лежит именно журнальное событие; нарушение —
  `EventStoreCorruptionError` (500), а не «пропустить с предупреждением», в
  отличие от политики декодера Kafka: локальный стрим обязан реплеиться целиком.
  Проигранная гонка за версию — `EventStoreConcurrencyError`
  (наследник `ConcurrentWriteError`); API теперь отображает `ConcurrentWriteError`
  в 409, а не в 500.
- **Разрыв цикла импортов.** `application/sagas/__init__.py` больше не
  реэкспортирует реестр: реестр импортирует всех консьюмеров, а консьюмер журнала
  импортирует пакет за базовым классом, поэтому реэкспорт замыкал цикл. Реестр
  импортируется по своему имени модуля (`plantkeeper.application.sagas.registry`),
  и его никто не импортировал из корня пакета.
- **Manual-entry endpoint не добавлен.** `AddJournalEntryCommand` реализован и
  покрыт тестами (им пользуется консьюмер), но `POST /api/v1/journal/{plant_id}` в
  роадмапе нет; записи `FERTILIZING`, `REPOTTING`, `NOTE` пока не имеют продюсера
  в Care, и это зафиксировано в `docs/event-sourcing.md`.
- **Строки `JournalEntryAdded` для skip/missed не пишутся.** `CareSkipped`,
  `CareMissed` и `WateringRescheduled` — про то, что ничего не сделали или что
  план сдвинулся; журнал фиксирует, что случилось.
- **`load_all` пока без вызывающего в проде.** Реализован и покрыт интеграционным
  тестом как точка входа для будущего replay/rebuild-инструмента.
- ADR не добавлялся: `0009-event-sourcing-journal.md` зарезервирован за Phase 10,
  решения фазы записаны в `docs/event-sourcing.md` и здесь (как в Phase 5). Новых
  зависимостей нет: только существующие SQLAlchemy, Dishka, Pydantic.
- Нумерация ADR не менялась: `0006-rest-and-grpc` (Phase 7), `0007-why-python-cqrs`,
  `0008-outbox-pattern`, `0009-event-sourcing-journal` (Phase 10).

---

## Phase 7 — gRPC 📡

> **Цель:** gRPC API для Care на том же application-слое.
> **Результат фазы:** gRPC-клиент может вызвать `GetTodayCare`.

> **Статус:** ✅ выполнено — `plantkeeper.v1.CareService` (`GetTodayCare`,
> `CompleteWatering`) и `plantkeeper.v1.GardenService` (`ListPlants`, `GetPlant`)
> обслуживаются `make grpc` на `0.0.0.0:50051` вместе с health-check'ом и
> reflection; оба протокола идут через один и тот же mediator. Стабы генерирует
> `make proto` (`tools/protogen.py`, `grpcio-tools`), он же — предпосылка
> `lint`/`test`/`test-unit`/`test-e2e`. `make lint` чист, `make test` — 648
> тестов (unit + integration + e2e на testcontainers), покрытие 96%. Коммиты
> созданы локально, push в `origin` не выполнялся.

### 7.1. Protobuf definitions
- [x] `proto/plantkeeper/v1/care.proto` (CareService: GetTodayCare, CompleteWatering) · `M`
- [x] `proto/plantkeeper/v1/garden.proto` (GardenService: ListPlants, GetPlant) · `M`
- [x] `proto/plantkeeper/v1/common.proto` (общие типы: PlantId, SensorReading) · `M`
- [x] `buf` конфиг или `grpcio-tools` Make-таргет · `M`
- [x] Генерация Python-стабов в `apps/api/src/.../grpc/generated/` · `M`
- [x] Коммит: `feat(proto): care and garden services` · `M`

### 7.2. gRPC server
- [x] `CareServicer` (grpc.aio) — вызывает QueryBus/CommandBus · `M` 🧪
- [x] `GardenServicer` · `M` 🧪
- [x] `server.py` — точка входа, DI, graceful shutdown · `M`
- [x] Health-check (`grpc.health.v1.Health`) · `S` 🧪
- [x] Reflection (для grpcurl) · `S`
- [x] Коммит: `feat(api): grpc server` · `L` 🧪

### 7.3. gRPC-клиент для тестов
- [x] Async gRPC client stub в `tests/` · `M` 🧪
- [x] E2E-тест: gRPC `GetTodayCare` → правильный ответ · `M` 🧪
- [x] Коммит: `test(e2e): grpc care service` · `M` 🧪

### 7.4. Документация
- [x] `docs/grpc.md` — как запустить, как вызвать через grpcurl · `M` 📝
- [x] ADR `0006-rest-and-grpc.md` — почему оба протокола · `M` 📝
- [x] Коммит: `docs: grpc` · `M` 📝

**✅ Phase 7 завершена, когда:** `grpcurl` из README работает, e2e-тест зелёный.

**Отклонения и уточнения:**
- **`buf` не используется, только `grpcio-tools`.** buf — второй бинарь, который
  пришлось бы ставить и пинить отдельно; `grpcio-tools` — уже питоновский тулинг и
  лежит в dev-группе рядом с Alembic. Причины и последствия — ADR 0006.
- **Стабы не коммитятся, и один их импорт переписывается.** protoc выводит путь
  модуля из пути proto-файла относительно include root, поэтому
  `plantkeeper/v1/common.proto` становится `plantkeeper.v1.common_pb2`, а соседние
  модули импортируют его как `from plantkeeper.v1 import common_pb2`. Стабы лежат
  в `plantkeeper.api.grpc.generated`, и `tools/protogen.py` переписывает этот
  префикс на реальный путь пакета. Опции для такого маппинга у protoc нет
  (`--python_opt=M...` `grpc_tools.protoc` отвергает), а `sys.modules`-алиас
  скрывал бы подмену; правка сгенерированного файла — честный минимальный способ.
- **`proto/` — контракт, но mypy его стабы не проверяет.** `grpcio` и `protobuf`
  не поставляют `py.typed`, поэтому strict-сборка видит их границы как `Any`
  (оверрайды `grpc.*`, `grpc_health.*`, `grpc_reflection.*`, `google.protobuf.*`),
  а сгенерированным модулям выставлен `ignore_errors`; аннотации вокруг этих
  границ — ручные. Компилируемый `--pyi_out` при этом типизирует все message-типы,
  которыми пользуются сервисеры.
- **Общий мост REST и gRPC вынесен в `plantkeeper.api.mediator`.** `build_mediator`
  и `view_of` переехали из FastAPI-зависимости `deps.py` (в нём осталась только
  `get_mediator`/`Mediator`), чтобы оба протокола конструировали mediator и
  проверяли обещанный тип ответа одним и тем же кодом.
- **Своя таблица gRPC-статусов.** В REST-слое ошибки отображаются в HTTP-статусы;
  для gRPC добавлен `plantkeeper.api.grpc.errors`: `NotFoundError` → `NOT_FOUND`,
  `IdempotencyKeyConflictError` → `ALREADY_EXISTS`, `ConcurrentWriteError` →
  `ABORTED`, `DomainError` → `FAILED_PRECONDITION`, `ValidationError` (кривое
  значение от клиента) → `INVALID_ARGUMENT`, всё остальное → `INTERNAL`. Разбор
  идентификатора выполняется *внутри* области отображения ошибок — иначе кривой
  UUID падал бы в `UNKNOWN`.
- **`SkipWatering` RPC не добавлен.** 7.1–7.2 называют для Care только
  `GetTodayCare` и `CompleteWatering`; пропуск полива остаётся на REST.
- **Сервер — отдельный процесс на `0.0.0.0:50051`.** Как `make api` (8000) и
  `make admin` (8001), порт — модульные константы в `grpc/server.py`, `Settings`
  не менялся. Процесс переиспользует `api_providers()` и, как REST-процесс, не
  строит Kafka-брокер: команды коммитят события в outbox, публикует их relay.
- **`SensorReading` есть в `common.proto`, но RPC его пока не отдаёт.** Роадмап
  7.1 называет его общим типом; чтобы он не был мёртвым, контрактный тест
  собирает и сериализует его. Поверхность телеметрии — задача более поздней фазы.
- **Клиент для тестов — фикстуры плюс сгенерированные стабы, а не библиотека.**
  7.3 просит «async gRPC client stub в `tests/`»: в `tests/e2e/conftest.py`
  появились `grpc_port` (сервер на свободном порту, Postgres-контейнер, без Kafka)
  и `grpc_channel`, а сами стабы — `CareServiceStub`/`GardenServiceStub` из
  `generated/`.
- **`make proto` — предпосылка `lint`/`test`/`test-unit`/`test-e2e`/`grpc`.**
  В свежем клоне стабов нет (они в `.gitignore`), и без этого шага импорт упал бы;
  Makefile делает шаг явным, а не прячет его в conftest.
- **Новые зависимости:** `protobuf` (рантайм сгенерированных модулей — `grpcio`
  его не тянет), `grpcio-health-checking`, `grpcio-reflection` в `apps/api`;
  `grpcio-tools` — в dev-группу корня, как Alembic.
- ADR 0006 использован по номеру, зарезервированному с Phase 2; нумерация
  последующих не менялась: `0007-why-python-cqrs`, `0008-outbox-pattern`,
  `0009-event-sourcing-journal` (Phase 10).

---

## Phase 8 — Notifications (HTTP long polling) 🔔

> **Цель:** клиент получает уведомления через long polling.
> **Результат фазы:** `GET /notifications/pending?timeout=30` висит до появления уведомления.

> **Статус:** ✅ выполнено — `NotificationChannel` (порт) и
> `ValkeyNotificationChannel` публикуют и ждут сигнал в канале `household:{id}`;
> `NotificationConsumer` превращает `WateringDue`, `WateringRescheduled`,
> `CareMissed`, `SoilMoistureLow` и `TemperatureAnomaly` в уведомления, а
> `NotificationPusher` будит ожидающий long poll; `GET
> /api/v1/notifications/pending` ждёт до `timeout` и отвечает `204`, когда ничего
> не появилось. Заодно `Sensor.record` вызывается ingress'ом, поэтому пороговые
> события наконец публикуются. `make lint` чист, `make test` — 671 тест (unit +
> integration + e2e на testcontainers), покрытие 96%. Коммиты созданы локально,
> push в `origin` не выполнялся.

### 8.1. Valkey Pub/Sub канал
- [x] `NotificationChannel` (publish/subscribe через asyncio) · `M` 🧪
- [x] При создании `Notification` — publish в канал `household:{id}` · `M` 🧪
- [x] Коммит: `feat(infra): notification channel` · `M` 🧪

### 8.2. Long polling endpoint
- [x] `GET /api/v1/notifications/pending?household_id=...&timeout=30` · `M` 🧪
- [x] Если есть непрочитанные — вернуть сразу · `S` 🧪
- [x] Если нет — subscribe Valkey, ждать с таймаутом · `M` 🧪
- [x] `POST /api/v1/notifications/{id}/ack` · `S` 🧪
- [x] Коммит: `feat(api): long polling notifications` · `M` 🧪

### 8.3. Notification consumer
- [x] Слушает `WateringDue`, `CareMissed`, `WateringRescheduled`, `SoilMoistureLow` · `M` 🧪
- [x] Создаёт `Notification` + publish в Valkey · `M` 🧪
- [x] Idempotency по event_id · `S` 🧪
- [x] Коммит: `feat(workers): notification consumer` · `M` 🧪

### 8.4. E2E-тест
- [x] Тест: событие → Notification в БД → long poll получает · `M` 🧪
- [x] Тест: таймаут → 204 No Content · `S` 🧪
- [x] Коммит: `test(e2e): long polling` · `M` 🧪

**✅ Phase 8 завершена, когда:** клиент получает уведомления без email/Telegram.

**Отклонения и уточнения:**
- **Publish в Valkey делает отдельный `NotificationPusher`, а не создатель
  уведомления.** Продюсеров уведомлений четыре (шаг онбординг-саги,
  `AdaptiveWateringSaga`, `NotificationConsumer`), и публикация «при создании»
  внутри их транзакций была бы и дублированием, и гонкой: ожидающий клиент
  проснулся бы раньше коммита строки. Pusher — обычный consumer на
  `notifications.events`: событие доходит до Kafka только через relay, то есть
  строго после коммита, и один код обслуживает все четыре продюсера.
- **Сигнал — это не данные.** В канал уходит только `household_id`; endpoint при
  пробуждении и по истечении таймаута перечитывает свои write-таблицы. Поэтому
  потерянный сигнал — это задержка на один интервал, а не потерянное уведомление,
  а неработающий Valkey не роняет delivery: pusher логирует и коммитит claim.
- **Endpoint отвечает `204`, а не `200 {"items": []}`.** Это осознанное изменение
  контракта (8.4 требует `204` на таймаут): «ничего нет» теперь одно
  представление, а не два. `timeout` по умолчанию `0` — обычный опрос отвечает
  сразу, long polling клиент включает явно; максимум 60 секунд, потому что
  запрос держит соединение с БД всё время ожидания.
- **`NotificationConsumer` слушает ещё и `TemperatureAnomaly`.** Роадмап 8.3
  перечисляет четыре события, но детектор (см. ниже) начинает публиковать и
  температурную аномалию; оставить событие без потребителя — против принципа
  «у каждого события есть consumer» из `docs/events.md`.
- **`care_missed` переехал из `MissedCareSaga` в `NotificationConsumer`.** Иначе
  тип получил бы двух писателей. Сага владеет расписанием и grace-окном, а
  Notifications — тем, что видит household. Тест 4.4 обновлён: сага проверяется по
  `CareMissed` в outbox, создание уведомления — тестом консьюмера.
- **Пороговый детектор появился здесь же.** `TelemetryIngestConsumer` теперь
  вызывает `Sensor.record(reading)` и складывает в outbox все события, которые
  поднял агрегат (`TelemetryReceived` + пороговое). Это закрывает
  `SoilMoistureLow`, `SoilMoistureHigh` и `TemperatureAnomaly`, которых не
  производил никто с Phase 5; `SoilMoistureHigh` теперь доходит до
  `AdaptiveWateringSaga` в проде. Строка сенсора по-прежнему не пишется:
  `last_seen_at` меняется только в памяти — решение Phase 5 сохранено.
  `SensorOffline` всё ещё без продюсера: ему нужен таймер тишины.
- **`soil_moisture_low` и `temperature_anomaly` создаются не чаще одной
  непрочитанной на растение.** Телеметрия идёт каждые 10 секунд; без этого
  ограничения (тем же, что Phase 4 применяет к переливу) засуха превратилась бы в
  поток уведомлений.
- **Идемпотентность тройная**: ledger `(consumer_group, event_id)`, id уведомления
  `uuid5` от события (восстановленная группа с потерянным ledger ничего не
  создаёт) и guard на непрочитанные. Это тот же приём, что у
  `JournalEntryConsumer`.
- **`payload` добавлен в HTTP-ответ.** Без него long poll не мог сказать, к
  какому растению относится `soil_moisture_low`.
- **ADR не добавлялся**: `0007` зарезервирован Phase 10.2 под `why-python-cqrs`;
  решения фазы записаны в `docs/notifications.md` и здесь (как в Phase 5 и 6).
  Новых зависимостей нет: `valkey` уже объявлен в `packages/infrastructure`,
  `testcontainers` уже умеет `ValkeyContainer`. Новых таблиц и миграций нет —
  `write_notifications.notifications` создана ещё в Phase 2.
- Нумерация ADR не менялась: `0007-why-python-cqrs`, `0008-outbox-pattern`,
  `0009-event-sourcing-journal` (Phase 10).

---

## Phase 9 — Trefle ACL 🌐

> **Цель:** реальная синхронизация каталога с внешним API.
> **Результат фазы:** SpeciesSyncSaga тянет данные из Trefle, Circuit Breaker защищает от падений.

> **Статус:** ✅ выполнено — `TrefleClient` пагинирует `/species`, тянет detail по каждому
> виду, держит лимит 55 req/min (`aiolimiter`), ретраит transient-ошибки (`tenacity`) и
> защищён проектным `AsyncCircuitBreaker` поверх порта `Clock`; `TrefleSpeciesSource`
> откатывается на последний снапшот в Valkey. `SpeciesSyncSaga` теперь создаёт незнакомые
> виды (`SpeciesAdded`, каталог 25 → 26) и обновляет изменившиеся, а её компенсация
> удаляет созданное. `ValkeySpeciesCache` (TTL 24ч) обслуживает `GetSpeciesQuery`, а
> `SpeciesCacheConsumer` роняет ключи на `SpeciesUpdated`/`SpeciesCacheInvalidated`.
> `make lint` и `make test` зелёные (unit 621, integration и e2e — см. блок ниже).
> Коммиты созданы локально, push в `origin` не выполнялся.

### 9.1. Trefle client (Anti-Corruption Layer)
- [x] `TrefleClient` (httpx.AsyncClient) · `M` 🧪
- [x] Модели ответов Trefle (Pydantic) · `M`
- [x] Маппинг Trefle → домен `Species` · `M` 🧪
- [x] Rate limiting (55 req/min) через `aiolimiter` · `M` 🧪
- [x] Коммит: `feat(infra): trefle acl` · `L` 🧪

### 9.2. Circuit Breaker + Retry
- [x] Circuit Breaker (tenacity / pybreaker) на TrefleClient · `M` 🧪
- [x] Retry с exponential backoff · `S` 🧪
- [x] Fallback: вернуть кэш из Valkey · `M` 🧪
- [x] Коммит: `feat(infra): circuit breaker for trefle` · `M` 🧪

### 9.3. Valkey cache
- [x] `SpeciesCache` (get/set/invalidate) с TTL 24ч · `M` 🧪
- [x] Кэш-хит в GetSpeciesQuery · `S` 🧪
- [x] Инвалидация при `SpeciesUpdated` · `S` 🧪
- [x] Коммит: `feat(infra): species cache` · `M` 🧪

### 9.4. Финальная интеграция SpeciesSyncSaga
- [x] Связать SpeciesSyncSaga с TrefleClient · `M` 🧪
- [x] Синхронизация 30 видов end-to-end · `M` 🧪
- [x] Коммит: `feat(application): species sync with trefle` · `M` 🧪

**✅ Phase 9 завершена, когда:** `POST /catalog/sync` подтягивает виды из Trefle, кэш работает.

**Отклонения и уточнения:**
- **Контракт Trefle сверен с живой документацией.** Списочные эндпоинты отдают только
  таксономию, без `growth`: вода и свет есть лишь в detail-запросе, по одному на вид.
  30 видов = 2 страницы списка + 30 detail = ~32 запроса при лимите 55/мин.
  Это зафиксировано в `docs/catalog.md`; тесты ходят через `httpx.MockTransport`, без сети.
- **Маппинг — эвристика по классам Элленберга.** `growth.light` (1–9) → `LOW`/`MEDIUM`/`HIGH`,
  `growth.soil_humidity` (1–12, именно 12) → фиксированные полосы 3/5/7/10/14/21 дней;
  `null` → неделя и `MEDIUM`. Слаги Trefle становятся `SpeciesId` через `uuid5` с
  неизменяемым namespace — это стык локального каталога и внешнего.
- **Circuit breaker свой, а не `pybreaker`.** Роадмап допускал `tenacity / pybreaker`;
  выбран проектный `AsyncCircuitBreaker` поверх порта `Clock`: таймаут reset проверяется
  сдвигом часов (как в Phase 4 вместо freezegun), нет новой зависимости, нет mypy-оверрайда
  (у `pybreaker.call_async` возврат `Any`). `tenacity` остался ретраем *внутри* breaker'а,
  поэтому серия ретраев — это одно логическое измерение отказа.
- **Fallback — снапшот последней удачной синхронизации**, а не отдельный per-species кэш:
  при недоступности Trefle или открытом breaker'е сага применяет `species:upstream:snapshot`
  из Valkey, а без снапшота — пустой список, то есть «изменений нет». Ошибка авторизации
  не маскируется. `fetch_all` никогда не возвращает частичный снапшот: 404/422 по отдельному
  виду пропускается, но недоступность сервиса обрывает весь fetch.
- **Создание вида потребовало нового события.** `SpeciesCreated` не существовал, поэтому
  добавлен `SpeciesAdded` (каталог 25 → 26) и `Species.add(...)`, по образцу `Plant.add`;
  `Species.create` по-прежнему не пишет событий (реконструкция и тесты). `SpeciesProjection`
  обрабатывает `SpeciesAdded` тем же upsert'ом, и read-модель впервые получила продюсера.
- **Инвалидация кэша событийная, а не вызовом внутри саги.** Шаг 3 саги по-прежнему пишет
  `SpeciesCacheInvalidated` в outbox (`OutboxSpeciesCache`, теперь узкий порт
  `SpeciesCacheInvalidator`), а `DEL` делает новый choreography-консьюмер
  `SpeciesCacheConsumer` (группа `<prefix>-species-cache`). Так ни один вызов Valkey не
  попадает в транзакцию БД, а последовательность шагов и тест компенсации Phase 4 не меняются.
  Окно «кэш ещё старый» между коммитом и консьюмером ограничено TTL (24ч) и задокументировано.
- **Компенсация создания удаляет строку, но не событие.** `SpeciesAdded` уже закоммичен в
  outbox и не может быть отозван — тот же случай, что `PlantOnboarded` в ADR 0005: read-модель
  может временно хранить вид, которого в write-таблице уже нет, до пересборки read-модели.
  Зафиксировано в `docs/catalog.md`; write-side каталог при этом возвращается к before-image.
- **Онбординг не ходит в Trefle.** `SpeciesCatalog` остаётся локальным (docstring порта
  поправлен): каталог наполняет плановая синхронизация, и сага онбординга не ждёт внешний HTTP.
- **Провайдеры DI переименованы/добавлены.** `NotificationProvider` → `ValkeyProvider`
  (один клиент Valkey; канал + `species_cache`), новый `ExternalProvider` (только в
  `worker_providers`: HTTP-клиент Trefle, лимитер, breaker, `SpeciesSource`) — API не строит
  Trefle-клиент. Реестр `CONSUMER_TYPES`/`SAGA_COMPONENT_TYPES` дополнен новым консьюмером,
  и добавлен тест, что реестр покрыт контейнером воркера.
- **Зависимость:** `aiolimiter>=1.3.0` в `packages/infrastructure` (как и называл роадмап;
  `py.typed`, поэтому без mypy-оверрайдов). `httpx` и `tenacity` уже были.
- **Новых таблиц и миграций нет**: кэш и снапшот живут в Valkey, `SpeciesAdded` едет по
  существующему `catalog.events`, read-модель `read_analytics.species` создана в Phase 3.
- ADR не добавлялся: `0007`–`0009` зарезервированы за Phase 10. Решения фазы записаны в
  `docs/catalog.md` и здесь (как в Phase 5, 6 и 8).
- **Группировка коммитов.** 9.3 и 9.4 ушли одним коммитом: шаг 3 саги, `providers.py`
  и сам порт `SpeciesCache` общие для обоих, и разрезать их значило бы оставить
  промежуточный коммит нерабочим. Итог: `feat(infra): trefle acl`,
  `feat(application): species sync with trefle` (включает кэш и его инвалидацию),
  `docs: catalog acl and cache close-out`. Circuit breaker попал в первый коммит,
  потому что клиент без него не собирается.

---

## Phase 10 — Polish ✨

> **Цель:** репозиторий готов к показу в портфолио.
> **Результат фазы:** AsyncAPI, OpenAPI, диаграммы, ADR, документация.

> **Статус:** ✅ выполнено — `make contracts` экспортирует `docs/openapi.json`,
> `docs/asyncapi-write.json` и `docs/asyncapi-read.json` из тех же регистраций, что
> обслуживают `make api`, `make workers` и `make admin`, и рендерит
> `docs/diagrams/event-flow.md` и `docs/diagrams/sagas.md` из реестров событий и
> списков шагов саг; `docs/architecture.md`, `docs/patterns.md` и ADR `0007`–`0009`
> на месте, README несёт финальную диаграмму. `make lint` чист, `make test` — 799
> тестов, покрытие 96% (domain 100%, application 97%, infrastructure 96%).
> Коммиты созданы локально, push в `origin` не выполнялся.

### 10.1. Контракты
- [x] AsyncAPI-экспорт из FastStream (`docs/asyncapi-write.json`, `docs/asyncapi-read.json`) · `M` 🧪
- [x] OpenAPI snapshot в `docs/openapi.json` · `S` 🧪
- [x] Генерация Mermaid-диаграмм из python-cqrs (`docs/diagrams/`) · `M` 🧪
- [x] Коммит: `feat(contracts): asyncapi and openapi export` · `M` 📝

### 10.2. Документация
- [x] `docs/architecture.md` — полный BC map + sequence diagrams · `L` 📝
- [x] `docs/patterns.md` — таблица паттернов с ссылками на код · `M` 📝
- [x] ADR `0007-why-python-cqrs.md` · `M` 📝
- [x] ADR `0008-outbox-pattern.md` · `M` 📝
- [x] ADR `0009-event-sourcing-journal.md` · `M` 📝
- [x] Обновить `README.md` финальной диаграммой · `M` 📝
- [x] Коммит: `docs: architecture, patterns, ADRs` · `L` 📝

### 10.3. Тестовое покрытие
- [x] Coverage report: domain ≥ 90% (факт 100%) · `M` 🧪
- [x] Coverage report: application ≥ 80% (факт 97%) · `M` 🧪
- [x] Coverage report: infrastructure ≥ 70% (факт 96%) · `M` 🧪
- [x] Отчёт в `docs/coverage.html` (gitignored артефакт) · `S`
- [x] Коммит: `test: coverage report` · `M`

### 10.4. Финальная вычитка
- [x] Пройтись по всем `TODO` в коде — их нет; отложенное зафиксировано в `docs/events.md` и `docs/sagas.md` · `M`
- [x] Проверить, что нет `print()`, `pdb.set_trace()`, закомментированного кода (единственный `print()` — вывод CLI `tools/protogen.py`) · `S`
- [x] `make lint && make test` — зелёные · `S`
- [x] Обновить `CHANGELOG.md` релизом `v0.1.0` · `S`
- [x] Тег `v0.1.0` · `S`
- [x] Коммит: `chore: release v0.1.0` · `S`

**✅ Phase 10 завершена, когда:** README рендерит полную диаграмму, все ADR на месте, `v0.1.0` тегирован.

**Отклонения и уточнения:**
- **Один AsyncAPI на процесс, а не один на платформу.** Роадмап называет
  `docs/asyncapi.json`; получилось `docs/asyncapi-write.json` (воркер) и
  `docs/asyncapi-read.json` (Django-проекции). Причина — контракт слоёв:
  `apps/admin` и `apps/workers` не могут импортировать друг друга, а генерация
  read-документа требует сконфигурированного Django. Один файл собирался бы
  процессом, который импортирует и Django, и воркер, то есть ровно там, где
  граница протекает. Оба файла пишутся одной командой `make contracts`; старый
  `docs/asyncapi.json` остаётся в `.gitignore` как алиас прошлых планов.
- **Контракты не коммитятся, диаграммы коммитятся.** `.gitignore` уже исключал
  `docs/openapi.json` и `docs/asyncapi.json` — тот же принцип, по которому в Phase 7
  не коммитятся gRPC-стабы: артефакт, который генерируется из кода, не должен
  расходиться с ним в истории. Mermaid-блок в Markdown читается на GitHub, поэтому
  `docs/diagrams/*.md` в индексе, а `make diagrams` их пересобирает; `--check` и тесты
  ловят расхождение. `docs/coverage.html` — тоже gitignored артефакт `make coverage`.
- **AsyncAPI описывает консьюмеров, каталог событий — расширением.** Продюсер
  write-стороны — это `OutboxRelay`, читающий `write_shared.outbox`, а не
  FastStream-роут, поэтому документация продюсеров недостижима из AsyncAPI. Оба
  документа несут `x-plantkeeper-event-catalogue` — 26 событий с топиком, продюсером
  и консьюмерами, собранных из `EVENT_TOPICS`, реестра саг и проекций.
- **Каналы получили заголовки.** FastStream берёт ключ AsyncAPI-канала из заголовка
  подписки, а иначе — из имени функции-обработчика; у всех обработчиков оно `handle`,
  поэтому несколько групп на одном топике схлопывались в один канал и документ молча
  терял все, кроме последнего. Подписки воркера и проекции админки теперь передают
  `title=<topic> to <consumer group>`, а `tests/unit/contracts/` падает на
  `RuntimeWarning` столкновения каналов.
- **Mermaid-диаграммы — из двух источников, а не только из python-cqrs.** Роадмап
  просит диаграммы из python-cqrs: `SagaMermaid` и даёт `docs/diagrams/sagas.md` из
  реальных списков шагов. Поток событий библиотека не описывает вовсе (`CoRMermaid`
  рисует цепочки `CORRequestHandler`, которых в проекте нет), поэтому
  `docs/diagrams/event-flow.md` строится из `EVENT_TOPICS` и реестров консьюмеров.
  Потребительские рёбра рисуются только между контекстами: событие, которое читает
  собственный контекст, — это норма, и рисовать её значило бы пересказать каталог.
- **`saga_name` объявлен на триггерах.** `OnboardPlantTrigger` и `SpeciesSyncTrigger`
  теперь называют сагу, которую запускают; это единственная правка application-слоя
  фазы. Без неё каталог не может подписать триггер сагой, не прибегая к совпадению
  индексов двух кортежей в реестре.
- **Пороговые флоры живут в `make coverage`, а не в `fail_under`.** pytest-cov берёт
  один `--cov`, поэтому разрезы по слоям считаются из сохранённого `.coverage`
  (`coverage report --include=...`) после полного прогона. Глобальный `fail_under`
  сломал бы `make test-unit` и `make test-domain`, которые покрывают по одному слою.
- **Новых зависимостей нет:** `faststream`, `fastapi`, `django` и `python-cqrs` уже
  объявлены; JSON пишется стандартной библиотекой, `pyyaml` не понадобился.
- Нумерация ADR не менялась: `0007-why-python-cqrs`, `0008-outbox-pattern`,
  `0009-event-sourcing-journal` использованы по номерам, зарезервированным с Phase 2.

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
