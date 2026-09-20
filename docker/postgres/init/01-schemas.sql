-- PlantKeeper — Postgres schemas.
--
-- One schema per write-side bounded context, plus `write_shared` for the tables
-- that serve every context (the transactional outbox and the HTTP idempotency
-- keys) and `read_analytics` for the projection/read models consumed by Django
-- Admin.
--
-- This script runs only when the Postgres data volume is initialised for the first
-- time (`/docker-entrypoint-initdb.d` semantics). Alembic creates the same schemas
-- idempotently in migration 0001, so an existing volume is migrated too; to re-run
-- both paths from scratch:
--
--     make clean && make dev && make migrate
--
-- Keep this file world-readable: the entrypoint runs it as the `postgres` user.

CREATE SCHEMA IF NOT EXISTS write_identity;
CREATE SCHEMA IF NOT EXISTS write_catalog;
CREATE SCHEMA IF NOT EXISTS write_garden;
CREATE SCHEMA IF NOT EXISTS write_care;
CREATE SCHEMA IF NOT EXISTS write_journal;
CREATE SCHEMA IF NOT EXISTS write_telemetry;
CREATE SCHEMA IF NOT EXISTS write_notifications;
CREATE SCHEMA IF NOT EXISTS write_shared;
CREATE SCHEMA IF NOT EXISTS read_analytics;
