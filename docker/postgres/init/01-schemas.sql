-- PlantKeeper — Postgres schemas of the write instance.
--
-- One schema per write-side bounded context, plus `write_shared` for the tables
-- that serve every context (the transactional outbox, the consumer ledger and the
-- saga state). The read side's schemas do not belong here: `read_analytics` and
-- `read_telemetry` are schemas of the read instance (`docker/postgres-read/init/`),
-- created by Django's own migrations.
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
