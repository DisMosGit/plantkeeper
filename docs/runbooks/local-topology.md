# Runbook: the local topology

The read side runs on its own Postgres instance. This runbook starts both
instances, proves the split is real, and collapses it back into one instance for a
small machine. It is the operational half of the read-side decision in
[ADR 0004](../adr/0004-read-side-projections.md), amended when the schema moved off
the write instance.

## What runs

| Service | Endpoint | Schemas |
|---------|----------|---------|
| `postgres` — the write instance | `localhost:5432` | `write_identity`, `write_catalog`, `write_garden`, `write_care`, `write_journal`, `write_telemetry`, `write_notifications`, `write_shared` |
| `postgres-read` — the read instance | `localhost:5433` | `read_analytics`, `read_telemetry`, plus Django's own `public` tables (`auth`, `sessions`, `django_migrations`) |
| Kafka (KRaft) | `localhost:9092` | — |
| Valkey | `localhost:6379` | — |
| Redpanda Console | <http://localhost:8080> | — |

Who opens which database:

- `make api` opens the write instance for every command, and a **second, read-only**
  engine on the read instance for the client list/report queries. That engine is
  lazy, so a process that never serves one never connects to the read instance.
- `make workers` opens the write instance only.
- `make admin` (the projections and Django Admin) opens the read instance only.

Both connections come from `Settings` — `POSTGRES_*` for the write instance and
`READ_POSTGRES_*` for the read one — read from the same `.env` the services and
`docker-compose.yml` use. The read engine opens its connections with
`default_transaction_read_only=on`, so "the read side does not write" is enforced by
Postgres rather than by convention.

## Start it

```bash
make dev      # the five containers, waiting until each reports healthy
make migrate  # Alembic on the write instance, then Django on the read instance
```

`make dev` is `docker compose up -d --wait --wait-timeout 300` and starts no
application process. `make migrate` runs `alembic upgrade head` against the write
instance and `python apps/admin/manage.py migrate` against the read instance.
Django's own migrations create `read_analytics` and `read_telemetry`, so the first
`make migrate` works even on a volume the Postgres init script never touched.

## Prove the split

```bash
docker compose ps
docker exec plantkeeper-postgres psql -U plantkeeper -d plantkeeper -c '\dn'
docker exec plantkeeper-postgres-read psql -U plantkeeper -d plantkeeper -c '\dn'
uv run python apps/admin/manage.py shell -c \
  "from django.db import connection; d = connection.settings_dict; print(d['HOST'], d['PORT'], d['NAME'])"
```

The read instance lists both read-side schemas:

```text
          List of schemas
      Name      |       Owner
----------------+-------------------
 public         | pg_database_owner
 read_analytics | plantkeeper
 read_telemetry | plantkeeper
```

On a volume created after the split, the write instance lists only `public` and the
`write_*` schemas. Django reports the read instance it is using:

```text
localhost 5433 plantkeeper
```

`docker compose ps` shows both `postgres` and `postgres-read` as `Up … (healthy)`;
`make dev` returned only after both healthchecks passed.

## Coming from a single-instance volume

The compose file used to run one Postgres holding `write_*` **and** `read_analytics`.
Switching to this layout reuses the existing volume, so the write instance keeps its
copy of `read_analytics` (tables and Django's `public.django_migrations` records
included). Nothing reads it — Django and the API's read engine address port 5433 —
and it is what makes the rollback below instant, because the read migrations are
already applied there. `\dn` on the write instance then still lists `read_analytics`;
that is the old copy, not the read side's.

A pristine split needs a fresh volume:

```bash
make clean     # stops the containers and removes the volumes — this destroys data
make dev
make migrate
```

## Roll back to one instance

The split is configuration, so the rollback is configuration too:

1. Point the read side at the write instance in `.env` — the port is the only
   difference with the defaults:

   ```dotenv
   READ_POSTGRES_PORT=5432
   ```

   If your write block differs from the defaults, set `READ_POSTGRES_HOST`,
   `READ_POSTGRES_USER`, `READ_POSTGRES_PASSWORD` and `READ_POSTGRES_DB` to the
   write values as well.

2. Apply the migrations: `make migrate`. On a volume that predates the split Django
   finds `read_models.0001` already applied; on a fresh volume it creates
   `read_analytics`, `read_telemetry` and their tables on the write instance.

3. Restart `make admin` and `make api` so they pick up the setting.

To check without starting a process:

```bash
READ_POSTGRES_PORT=5432 uv run python apps/admin/manage.py showmigrations read_models read_telemetry
```

```text
read_models
 [X] 0001_read_models
read_telemetry
 [X] 0001_initial
```

The `postgres-read` container is now unused; `docker compose stop postgres-read`
stops it without touching its data. Rolling forward again is the same steps with
`READ_POSTGRES_PORT=5433` restored.

While the two sides share one instance, a projection rebuild truncates
`read_analytics` (or `read_telemetry`) in the same database the write path uses — the
competition the split exists to remove, and the reason the retention window's dropped
partitions and the rollups would share one disk. It is a deliberate trade for a machine
that cannot run two Postgres instances.

## When something is wrong

- **Port 5433 is taken.** Change the published port in `docker-compose.yml`
  (`postgres-read` → `ports`) and `READ_POSTGRES_PORT` in `.env` together; the
  container always listens on 5432 internally.
- **The read instance is down.** Commands still succeed on the write instance, and
  only the list/report queries fail, because the read engine is opened on demand —
  `tests/e2e/test_query_split.py` pins that the moved queries are answered with the
  write instance unreachable, not the other way round.
- **Which database did a query hit?** The read-side processes answer from port 5433.
  A query that reaches the write instance is a bug in the query split, not a topology
  problem ([`docs/cqrs.md`](../cqrs.md)); the read instance's own connections are
  visible with:

  ```bash
  docker exec plantkeeper-postgres-read psql -U plantkeeper -d plantkeeper -c \
    'SELECT count(*) FROM pg_stat_activity'
  ```
- **A projection needs rebuilding.** That operates on the read instance only; the
  procedure is in [`docs/cqrs.md`](../cqrs.md#rebuilding-a-read-model).
