# Notifications

> **Status:** Phase 11. The producer is `NotificationConsumer` and the signal is
> `NotificationPusher`, both in `apps/workers`; the client-facing half is
> `GET /api/v1/notifications/stream` (server-sent events) with
> `GET /api/v1/notifications/pending` kept as the request-and-wait fallback, both
> in `apps/api`. There is no email and no Telegram: a household's notifications
> are delivered to whoever is connected to the API.

## Delivery: a stream, with request and wait as fallback

Both forms are woken by the same payload-free household signal and both answer a
wake-up from the write tables; they differ only in what the client sees. The
stream is the primary form — it pushes each notification as it appears and
resumes where it stopped — and the request-and-wait endpoint is the fallback for
a client that cannot hold a connection open.

### The stream (primary)

```
GET /api/v1/notifications/stream?household_id=<uuid>&since=<notification-id>
Accept: text/event-stream
```

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `household_id` | — | whose notifications to stream (required) |
| `since` | — | the last notification the client saw; omit it for everything unread |

`Last-Event-ID` is the same cursor: an SSE client that reconnects after a dropped
connection sends the id of the last frame it read, and the server resumes there.
An explicit `since` wins over the header.

The response is `text/event-stream`, one frame per line group:

```
retry: 3000

: stream open

id: 0198f2c1-…
event: notification
data: {"notification_id":"…","household_id":"…","notification_type":"soil_moisture_low","payload":{…},"created_at":"…","read_at":null}

: keep-alive
```

| Frame | Meaning |
|-------|---------|
| `event: notification` | one unread notification; its payload is exactly a `/pending` item |
| `: stream open` | the household's signal is attached; the stream is live |
| `: signal unavailable: answering from storage` | the signal could not be reached: what follows is a plain read and the response ends |
| `: keep-alive` | nothing happened for 15 seconds; the stream is still open |
| `retry: 3000` | the first line: how long to wait before reconnecting |

The cursor is what makes a reconnect safe. Notification ids are UUIDv7, so they
sort by creation; each frame carries its id, and the stream only ever sends what
comes after the cursor it has advanced to. A client that reconnects naming its
last notification therefore receives what it missed — and nothing it already
saw — while an unacknowledged notification it has already seen is not repeated.

An idle stream holds **one household signal subscription and no database
session**. The subscription is shared by every open stream of that household —
one per household, whatever the number of connections — and the stream reads on a
session of its own after each wake-up, releasing it again immediately.

### Request and wait (fallback)

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

Try both against a running stack:

```bash
make dev && make migrate && make api && make workers
curl -N "http://localhost:8000/api/v1/notifications/stream?household_id=<uuid>"
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
4. the stream or the waiting request re-reads **its own database** and answers with
   whatever it finds.

Because step 3 runs strictly after the commit in step 1, a woken reader always
finds the row. Because step 4 reads the database rather than the channel, a lost
nudge costs one wake-up interval of latency and a duplicated one costs a redundant
query: the stream re-reads after every wait — a nudge, or its own keep-alive
interval — and the fallback re-reads after every wake and once more when its
deadline passes. Nothing is ever delivered exclusively through Valkey, and a
Valkey outage costs a plain read instead of losing notifications.

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
| `sensor_offline` | `SensorOffline` | `NotificationConsumer` |
| `temperature_anomaly` | `TemperatureAnomaly` | `NotificationConsumer` |
| `soil_moisture_high` | `SoilMoistureHigh` | `AdaptiveWateringSaga` |

`care_missed` moved out of `MissedCareSaga` in Phase 8: the saga owns the schedule
and the grace window, and the Notifications context owns what the household sees.

`SoilMoistureLow`, `SoilMoistureHigh`, `TemperatureAnomaly` and `SensorOffline` only
had a producer from Phase 8 on: the telemetry ingress calls `Sensor.record(...)` with
each new reading, which raises the threshold events the aggregate's rules imply, and
`SensorSilenceJob` calls `Sensor.mark_offline(...)` once per silence. The ingress also
persists the sensor's `last_seen_at` now, in the same transaction as the reading — the
silence timer reads it, and a sensor that has reported again has cleared its
announcement.

`NotificationConsumer` also subscribes to `garden.events`, for the one fact it needs
and does not own: which household to address. `PlantAdded`, `PlantMoved` and
`PlantRemoved` maintain `write_notifications.plant_refs`, this context's own row
(`plantkeeper.application.references`), and every reminder is addressed from it. A
trigger for a plant this context has not seen is logged and dropped: there is no
household to address, and none is invented.

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
   need no guard, and neither does `sensor_offline` — its producer already announces
   a silence once, so a second event means the sensor reported and went quiet again,
   which is a new fact the household should see.

## Configuration

| Variable | Default | Meaning |
|----------|---------|---------|
| `VALKEY_URL` | `valkey://localhost:6379/0` | the presence channel |

The wait bounds are module constants in the router
(`DEFAULT_POLL_TIMEOUT_SECONDS`, `MAX_POLL_TIMEOUT_SECONDS`), like the sagas'
grace period: how long a client may wait is a policy of the endpoint, not a
property of the environment. The stream's own intervals are constants of the
application's stream service too (`KEEP_ALIVE_SECONDS`, and the fan-out's
`NUDGE_WAIT_SECONDS`), with the SSE `retry` hint in the router's `sse` module.

## Known limits

- The fallback holds one request-scoped database session for the whole wait, and
  one Valkey subscription per waiting client. Its 60-second cap bounds both; a
  deployment that expects many simultaneous waiters should prefer the stream, and
  raise the connection pool rather than the timeout.
- The stream holds one subscription per household, however many streams that
  household has open, and no database session while it is idle. Its reads are one
  short session each, so a household with thousands of open streams still costs
  one subscription and no idle connections.
- The signal is per household, not per notification type, so every stream and
  waiter of a household wakes for every change to it. That is the point — one
  reminder is as good a reason to look as another — but it does mean a chatty
  plant wakes its neighbours in the same household.
