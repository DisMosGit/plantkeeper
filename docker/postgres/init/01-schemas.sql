-- PlantKeeper — Postgres schemas.
--
-- One schema per write-side bounded context, plus `read_analytics` for the
-- projection/read models consumed by Django Admin.
--
-- This script runs only when the Postgres data volume is initialised for the first
-- time (`/docker-entrypoint-initdb.d` semantics). To re-run it:
--
--     make clean && make dev
--
-- Keep this file world-readable: the entrypoint runs it as the `postgres` user.

CREATE SCHEMA IF NOT EXISTS write_identity;
CREATE SCHEMA IF NOT EXISTS write_catalog;
CREATE SCHEMA IF NOT EXISTS write_garden;
CREATE SCHEMA IF NOT EXISTS write_care;
CREATE SCHEMA IF NOT EXISTS write_journal;
CREATE SCHEMA IF NOT EXISTS write_telemetry;
CREATE SCHEMA IF NOT EXISTS write_notifications;
CREATE SCHEMA IF NOT EXISTS read_analytics;
