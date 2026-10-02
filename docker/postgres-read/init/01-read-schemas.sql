-- PlantKeeper — Postgres schemas of the read instance.
--
-- The read side owns its own database (`docker-compose.yml`'s `postgres-read`) and
-- creates its schemas there and nowhere else: `read_analytics` holds the projection
-- read models Django Admin shows, and `read_telemetry` holds the telemetry rollups
-- and the latest-per-sensor row.
--
-- This script runs only when the read instance's data volume is initialised for the
-- first time (`/docker-entrypoint-initdb.d` semantics). Django's own migrations
-- create the same schemas idempotently, so an existing volume is migrated too; to
-- re-run both paths from scratch:
--
--     make clean && make dev && make migrate
--
-- Keep this file world-readable: the entrypoint runs it as the `postgres` user.

CREATE SCHEMA IF NOT EXISTS read_analytics;
CREATE SCHEMA IF NOT EXISTS read_telemetry;
