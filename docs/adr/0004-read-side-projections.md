# 4. The read side: projections, schema and Django Admin

## Status

Accepted (Phase 3)

## Date

2026-09-20

## Context

Phase 3 has to make one promise: a plant created through `POST /api/v1/plants` is
visible in Django Admin. Between those two facts sit four decisions that were not
made by the write side and could each be made badly:

1. **Where the projections run.** The read models are Django models, and Kafka
   consumers need a process. There are three deployables (`apps/api`,
   `apps/workers`, `apps/admin`) and the import-linter contract keeps them
   mutually independent, so "the read side" has to be exactly one of them.
2. **Who owns the read schema's DDL.** The write side has Alembic and eight
   `write_*` schemas. The read side needs tables in the same database. Two
   migration tools touching one database is a smell unless the lanes are clear.
3. **How a projection is idempotent.** Delivery is at-least-once by design
   ([ADR 0003](0003-write-side-outbox.md)), and `AGENTS.md` requires every
   consumer to be idempotent on `(consumer_group, event_id)`. A read model that
   is assembled from several topics makes that harder than it sounds: the events
   that fill one row do not arrive in a guaranteed order.
4. **How Django Admin presents itself without authentication.** `AGENTS.md` and
   `README.md` say the project has none, and Django Admin cannot render without
   `django.contrib.auth` and a user.

## Decision

**The read side is `apps/admin`, and it runs the projections.** Its dependency
set already says so: it is the only app with `django`, `starlette` **and**
`faststream[kafka]`. `make admin` starts one process that subscribes to the event
topics and serves Django Admin; `apps/workers` stays what Phase 2 made it — the
write side's relay. The alternative (projections in `apps/workers`, writing the
read schema through SQLAlchemy Core while Django models mirrored the tables for
the UI) was rejected because it defines every table twice and needs a parity test
to stop the two definitions drifting.

**Django owns `read_analytics`.** The schema is one more Postgres schema in the
same database, created by `apps/admin`'s migration `0001_read_models` and never
touched by Alembic. Alembic owns `write_*`; Django owns `read_analytics`; the two
tools do not overlap. The migration creates the schema itself because
`docker/postgres/init` only runs on a fresh volume and the test suite starts an
empty container with no init script at all.

**One writer per column, and the ledger commits with the write.** Every read
column has exactly one owning projection. Where that is not enough — a
`plants` row is filled by Garden, Care and Catalog, whose events travel on three
topics with independent consumers — the rule is instead: *a projection may create
the row and write only the columns it owns*. `update_or_create` writes the fields
it is given and leaves the rest, so the order in which the three topics are
consumed cannot lose a fact, and the garden-owned columns are nullable so a care
or journal event for a plant whose `PlantAdded` has not been projected yet still
produces a (partial, transient) row. Idempotency is a
`read_analytics.processed_events(consumer_group, event_id)` row written **inside
the same transaction** as the projection's writes: a redelivery finds it and
stops, and a failure rolls the claim back with the write, so the retry starts
from nothing.

**One consumer group per projection**, named
`<READ_SIDE_CONSUMER_GROUP_PREFIX>-<projection>`, with `auto_offset_reset="earliest"`.
A fresh group replays its topic from the beginning, which is what makes a rebuilt
read table possible; the ledger is what makes the replay harmless.

**Django Admin auto-selects a local superuser.** `DevAutoLoginMiddleware` (after
`AuthenticationMiddleware`) signs each request in as one fixed local user, created
on demand with an unusable password. There is no login form to reach — Django
redirects an already-authenticated staff user away from `/admin/login/` — and
`DJANGO_AUTO_LOGIN_USER=` (empty) removes the middleware's effect entirely. The
contrib apps are still installed because Admin needs them; what the project does
not have is *application* authentication, which is what the README and `AGENTS.md`
mean. Every `ModelAdmin` here is read-only: a row appears because an event was
projected, so an edit would be overwritten and a deletion would come back on the
next replay.

## Consequences

- **Easier:** the CQRS split is a deployable split, not a naming convention.
  `make api` writes, `make workers` publishes, `make admin` projects and shows.
- **Easier:** one schema per migration tool. A failing Alembic upgrade cannot
  touch a read table, and a failing Django migration cannot touch a write table.
- **Easier:** a stuck projection is diagnosable from the ledger
  (`/admin/read_models/processedevent/`, filtered by consumer group).
- **Harder:** a projection is a *reader* of the event contract, so it has to
  deserialise without an envelope. `EVENT_TYPES` (in
  `infrastructure/messaging/topics.py`) is the name-to-type registry both this and
  future consumers use.
- **Harder:** a read model assembled from several topics can be briefly partial.
  The columns are nullable and the admin renders `—`; the alternative was a read
  model that joins across the CQRS split.
- **Harder:** the read side is a second dialect of persistence in one repository —
  SQLAlchemy for writing, Django ORM for reading. That is the stack the README
  already names, and the layers keep them apart: `plantkeeper.admin` may import
  `plantkeeper.infrastructure`, never the reverse.
- **Constrained:** nothing on the read side may read a `write_*` table. A read
  model that joins the write side is not a projection; it is a second consumer of
  the write schema, and it would break the moment the two are separated.
- **Constrained:** every new projection must be added to `ALL_PROJECTIONS` and
  covered by the catalogue contract test, or its events are silently ignored.
- **Follow-up:** nothing publishes `JournalEntryAdded` or `SpeciesUpdated` before
  Phases 6 and 9, so `plants.species_name` stays empty until the Catalog publishes
  a species. That is a gap in the *producers*, and the read side is ready for them.
- **Follow-up:** telemetry has no read model yet (Phase 5); the catalogue contract
  test names `TelemetryReceived` and its four siblings as unprojected on purpose.

## References

- [`docs/cqrs.md`](../cqrs.md) — the schema map, the projection table and how to
  rebuild the read side
- [`docs/events.md`](../events.md) — the event catalogue and its Kafka transport
- [`docs/adr/0003-write-side-outbox.md`](0003-write-side-outbox.md) — why delivery
  is at-least-once, and therefore why the ledger exists
- [`apps/admin/src/plantkeeper/admin/projections/base.py`](../../apps/admin/src/plantkeeper/admin/projections/base.py)
  — the idempotency transaction
- [`apps/admin/src/plantkeeper/admin/read_models/models.py`](../../apps/admin/src/plantkeeper/admin/read_models/models.py)
  — the read models and the column ownership comments
- [`apps/admin/src/plantkeeper/admin/dev_auth.py`](../../apps/admin/src/plantkeeper/admin/dev_auth.py)
  — the auto-login middleware
