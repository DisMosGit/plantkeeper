# Notifications

> **Status:** Phase 8. The producer is `NotificationConsumer` and the signal is
> `NotificationPusher`, both in `apps/workers`; the client-facing half is
> `GET /api/v1/notifications/pending` in `apps/api`. There is no email and no
> Telegram: a household's notifications are delivered to whoever is long-polling.

## The endpoint

```
GET /api/v1/notifications/pending?household_id=<uuid>&timeout=30
```

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `household_id` | — | whose notifications to read (required) |
| `timeout` | `0` | seconds to wait for one when there are none yet (0–60) |

| Answer | When |
|--------|------|
| `200` + `{"items": […]}` | something is pending (immediately, or after the wait) |
| `204 No Content` | nothing appeared within `timeout` |
| `422` | `timeout` outside 0–60 |

A notification response carries its payload, so a client knows *which* plant the
reminder is about:

```json
{"notification_id":"…","household_id":"…","notification_type":"soil_moisture_low",
 "payload":{"plant_id":"…","moisture":12.0,"threshold":30.0},
 "created_at":"…","read_at":null}
```

`timeout=0` is the default on purpose: the endpoint answers a plain poll
immediately, and long polling is something a client opts into. `POST
/api/v1/notifications/{id}/ack` acknowledges one and answers `409` when it was
already acknowledged — that is the aggregate's rule, not an idempotency shortcut.

Try it against a running stack:

```bash
make dev && make migrate && make api && make workers
curl -N "http://localhost:8000/api/v1/notifications/pending?household_id=<uuid>&timeout=30"
make iot-drought   # in another terminal: dry soil raises soil_moisture_low
```

## The channel carries a nudge, not the data

The write side must be able to say "look again" without knowing who is listening,
so the two halves are deliberately asymmetric:

1. a fact (`WateringDue`, `CareMissed`, …) becomes a `Notification` **row** in
   `write_notifications.notifications`, in the transaction that claims the
   delivery;
2. its `NotificationCreated` event is committed to the outbox with it, and the
   relay publishes it to `notifications.events`;
3. `NotificationPusher` consumes that event and publishes the household id on the
   Valkey channel `household:<id>` — a wake-up, with no payload;
4. the waiting endpoint re-reads **its own database** and answers with whatever it
   finds.

Because step 3 runs strictly after the commit in step 1, a woken poller always
finds the row. Because step 4 reads the database rather than the channel, a lost
nudge costs one poll interval of latency and a duplicated one costs a redundant
query. Nothing is ever delivered exclusively through Valkey, and the endpoint also
re-reads once more when its deadline passes, so a Valkey outage degrades latency
instead of losing notifications.

The channel is best effort in the other direction too: if `publish` fails, the
pusher logs a warning and commits the delivery rather than pinning its consumer
group on an unreachable Valkey.

## Who creates which notification

One producer per type, so no type has two writers that could disagree:

| Type | Trigger event | Producer |
|------|---------------|----------|
| `plant_onboarded` | — (saga step) | `OnboardPlantSaga` |
| `watering_due` | `WateringDue` | `NotificationConsumer` |
| `watering_rescheduled` | `WateringRescheduled` | `NotificationConsumer` |
| `care_missed` | `CareMissed` | `NotificationConsumer` |
| `soil_moisture_low` | `SoilMoistureLow` | `NotificationConsumer` |
| `temperature_anomaly` | `TemperatureAnomaly` | `NotificationConsumer` |
| `soil_moisture_high` | `SoilMoistureHigh` | `AdaptiveWateringSaga` |

`care_missed` moved out of `MissedCareSaga` in Phase 8: the saga owns the schedule
and the grace window, and the Notifications context owns what the household sees.

`SoilMoistureLow`, `SoilMoistureHigh` and `TemperatureAnomaly` only had a producer
from Phase 8 on: the telemetry ingress now calls `Sensor.record(...)` with each new
reading, which raises the threshold events the aggregate's rules imply. The reading
itself is still the only row written — the sensor's `last_seen_at` is updated in
memory only, as Phase 5 decided.

## Idempotency

Three rules, in this order:

1. **The ledger.** `NotificationConsumer` and `NotificationPusher` are ordinary
   write-side consumers: the delivery is claimed in
   `write_shared.processed_events` on `(consumer_group, event_id)` in the same
   transaction as their work.
2. **A derived identifier.** A notification's id is
   `uuid5(NAMESPACE_URL, "plantkeeper:notification:<event_name>:<event_id>")`, and
   the consumer skips an id it already stored. A rebuilt consumer group — a reset
   offset plus a lost ledger — therefore re-creates nothing.
3. **An unread guard for telemetry.** `soil_moisture_low` and
   `temperature_anomaly` are created at most once per plant *while unread*: a
   sensor reports every ten seconds, and a reminder that is already on screen does
   not need repeating. `watering_due`-style facts are naturally once-per-fact and
   need no guard.

## Configuration

| Variable | Default | Meaning |
|----------|---------|---------|
| `VALKEY_URL` | `valkey://localhost:6379/0` | the presence channel |

The wait bounds are module constants in the router
(`DEFAULT_POLL_TIMEOUT_SECONDS`, `MAX_POLL_TIMEOUT_SECONDS`), like the sagas'
grace period: how long a client may wait is a policy of the endpoint, not a
property of the environment.

## Known limits

- A long poll holds one request-scoped database session for the whole wait, and
  one Valkey subscription per waiting client. The 60-second cap bounds both; a
  deployment that expects many simultaneous waiters should raise the connection
  pool rather than the timeout.
- The signal is per household, not per notification type, so every waiter of a
  household wakes for every change to it. That is the point — one reminder is as
  good a reason to look as another — but it does mean a chatty plant wakes its
  neighbours in the same household.
- `SensorOffline` has no producer yet: it needs a silence timer over
  `last_seen_at`, which is a later phase's job.
