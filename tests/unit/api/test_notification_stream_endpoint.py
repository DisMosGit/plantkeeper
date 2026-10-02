"""The notification stream's HTTP surface, without Docker.

Three things live here: the SSE frame format, the route as the application serves
it (over a scripted channel and reader, so the stream is finite and the ASGI
transport can finish it), and the OpenAPI descriptions of the two delivery forms.
The live stream — a real connection woken by a real nudge — is
``tests/e2e/test_notification_stream.py``'s job.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest
from dishka import Provider, Scope, provide
from httpx import ASGITransport, AsyncClient

from plantkeeper.api.main import create_app
from plantkeeper.api.openapi import build_document
from plantkeeper.api.rest import sse
from plantkeeper.application.ports.notifications import (
    NotificationChannel,
    NotificationChannelError,
    PendingNotificationReader,
)
from plantkeeper.application.views import NotificationView
from plantkeeper.domain.identifiers import HouseholdId, NotificationId, PlantId
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.infrastructure.di.providers import api_providers

HOUSEHOLD = HouseholdId.new()
STREAM_PATH = "/api/v1/notifications/stream"
PENDING_PATH = "/api/v1/notifications/pending"


# --- the frame format ---------------------------------------------------------


def test_an_event_frame_carries_its_id_its_name_and_its_payload() -> None:
    assert sse.sse_event(data='{"a":1}', event="notification", event_id="abc") == (
        'id: abc\nevent: notification\ndata: {"a":1}\n\n'
    )


def test_a_multiline_payload_stays_one_frame() -> None:
    """A newline in the payload would otherwise start a second, malformed event."""
    assert sse.sse_event(data="a\nb") == "data: a\ndata: b\n\n"


def test_a_comment_and_a_retry_are_frames_of_their_own() -> None:
    assert sse.sse_comment("keep-alive") == ": keep-alive\n\n"
    assert sse.sse_retry(sse.RETRY_MILLISECONDS) == f"retry: {sse.RETRY_MILLISECONDS}\n\n"


# --- the route ----------------------------------------------------------------


def a_view() -> NotificationView:
    """One pending notification, as the reader would hand it over."""
    return NotificationView(
        notification_id=NotificationId.new(),
        household_id=HOUSEHOLD,
        notification_type=NotificationType.WATERING_DUE,
        payload={"plant_id": str(PlantId.new())},
        created_at=datetime.now(UTC),
        read_at=None,
    )


class ListReader:
    """The stream's read, over a list the test owns."""

    def __init__(self, pending: list[NotificationView] | None = None) -> None:
        self.pending = list(pending or [])
        self.cursors: list[NotificationId | None] = []

    async def pending_since(
        self, household_id: HouseholdId, *, since: NotificationId | None
    ) -> list[NotificationView]:
        """Record the cursor, and answer the way the repository does."""
        self.cursors.append(since)
        return [
            item
            for item in self.pending
            if since is None or item.notification_id.value > since.value
        ]


class DyingSubscription:
    """A subscription that dies shortly after it opens.

    The ASGI transport buffers a response until the application finishes it, so a
    route test can only read a stream that ends; this is what makes it end. The
    small delay is what lets the route finish attaching — and so emit its opening
    comment — before the loss arrives, exactly as a Valkey that dies a moment
    after a client subscribes would.
    """

    async def wait(self, timeout: float) -> bool:
        await asyncio.sleep(0.05)
        raise NotificationChannelError("valkey went away")


class ScriptedChannel:
    """A presence channel whose one subscription is available, or is not."""

    def __init__(self, *, available: bool = True) -> None:
        self.available = available
        self.opened = 0

    @asynccontextmanager
    async def subscribe(self, household_id: HouseholdId) -> AsyncIterator[DyingSubscription]:
        """Open the household's signal, or fail the way an unreachable Valkey does."""
        if not self.available:
            raise NotificationChannelError("valkey is down")
        self.opened += 1
        yield DyingSubscription()

    async def publish(self, household_id: HouseholdId) -> None:
        """Unused: the reader is what these tests drive."""


class ScriptedProvider(Provider):
    """The API container with the stream's I/O replaced.

    The channel and the reader are the only things overridden, and both are
    registered by ``api_providers`` already: the service, the route and the
    document stay the real ones, so these tests fail when the wiring does.
    """

    def __init__(self, channel: ScriptedChannel, reader: ListReader) -> None:
        super().__init__()
        self._channel = channel
        self._reader = reader

    @provide(scope=Scope.APP, override=True)
    def notification_channel(self) -> NotificationChannel:
        return self._channel

    @provide(scope=Scope.APP, override=True)
    def pending_notification_reader(self) -> PendingNotificationReader:
        return self._reader


@asynccontextmanager
async def a_client(channel: ScriptedChannel, reader: ListReader) -> AsyncIterator[AsyncClient]:
    """An HTTP client over the real application and the scripted stream I/O."""
    app = create_app(providers=[*api_providers(), ScriptedProvider(channel, reader)])
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


def notification_frames(body: str) -> list[tuple[str, dict[str, object]]]:
    """Return ``(id, payload)`` for every notification frame in an SSE body."""
    frames: list[tuple[str, dict[str, object]]] = []
    current: dict[str, str] = {}
    for line in [*body.splitlines(), ""]:
        if line == "":
            if current.get("event") == "notification" and "data" in current:
                payload = json.loads(current["data"])
                assert isinstance(payload, dict)
                frames.append((current.get("id", ""), payload))
            current = {}
            continue
        if line.startswith(":"):
            continue
        name, _, value = line.partition(": ")
        current[name] = current.get(name, "") + value
    return frames


async def test_the_route_streams_a_notification_as_an_sse_frame() -> None:
    notification = a_view()
    reader = ListReader([notification])
    channel = ScriptedChannel()

    async with a_client(channel, reader) as client:
        response = await client.get(STREAM_PATH, params={"household_id": str(HOUSEHOLD.value)})

    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache"
    assert response.text.startswith(f"retry: {sse.RETRY_MILLISECONDS}\n\n")
    assert ": stream open" in response.text

    [(event_id, payload)] = notification_frames(response.text)
    assert event_id == str(notification.notification_id.value)
    assert payload["notification_id"] == str(notification.notification_id.value)
    assert payload["notification_type"] == NotificationType.WATERING_DUE.value
    assert payload["payload"] == notification.payload
    assert channel.opened == 1


async def test_the_route_resumes_from_the_since_parameter() -> None:
    cursor = NotificationId.new()
    reader = ListReader()

    async with a_client(ScriptedChannel(), reader) as client:
        response = await client.get(
            STREAM_PATH,
            params={"household_id": str(HOUSEHOLD.value), "since": str(cursor.value)},
        )

    assert response.status_code == 200, response.text
    assert reader.cursors[0] == cursor
    assert set(reader.cursors) == {cursor}, "every read of this stream resumes from the cursor"


async def test_the_route_resumes_from_last_event_id_when_no_since_is_given() -> None:
    """An SSE client that reconnects sends the header, not a query parameter."""
    header_cursor = NotificationId.new()
    reader = ListReader()

    async with a_client(ScriptedChannel(), reader) as client:
        response = await client.get(
            STREAM_PATH,
            params={"household_id": str(HOUSEHOLD.value)},
            headers={"Last-Event-ID": str(header_cursor.value)},
        )

    assert response.status_code == 200, response.text
    assert reader.cursors[0] == header_cursor

    explicit = NotificationId.new()
    reader.cursors.clear()
    async with a_client(ScriptedChannel(), reader) as client:
        await client.get(
            STREAM_PATH,
            params={"household_id": str(HOUSEHOLD.value), "since": str(explicit.value)},
            headers={"Last-Event-ID": str(header_cursor.value)},
        )

    assert reader.cursors[0] == explicit, "an explicit since must win over the header"


async def test_the_route_degrades_to_a_plain_read_without_the_signal() -> None:
    notification = a_view()
    reader = ListReader([notification])
    channel = ScriptedChannel(available=False)

    async with a_client(channel, reader) as client:
        response = await client.get(STREAM_PATH, params={"household_id": str(HOUSEHOLD.value)})

    assert response.status_code == 200, response.text
    assert ": signal unavailable" in response.text
    [(event_id, payload)] = notification_frames(response.text)
    assert event_id == str(notification.notification_id.value)
    assert payload["notification_id"] == str(notification.notification_id.value)
    assert channel.opened == 0


@pytest.mark.parametrize("path", [STREAM_PATH, PENDING_PATH])
async def test_both_delivery_forms_reject_a_missing_household(path: str) -> None:
    async with a_client(ScriptedChannel(), ListReader()) as client:
        response = await client.get(path)

    assert response.status_code == 422


# --- the document -------------------------------------------------------------


def test_the_document_documents_both_delivery_forms() -> None:
    paths = build_document()["paths"]
    assert isinstance(paths, dict)
    stream = paths[STREAM_PATH]
    assert isinstance(stream, dict)
    stream_get = stream["get"]
    assert isinstance(stream_get, dict)
    pending = paths[PENDING_PATH]
    assert isinstance(pending, dict)
    pending_get = pending["get"]
    assert isinstance(pending_get, dict)

    responses = stream_get["responses"]
    assert isinstance(responses, dict)
    accepted = responses["200"]
    assert isinstance(accepted, dict)
    content = accepted["content"]
    assert isinstance(content, dict)
    assert "text/event-stream" in content

    stream_description = stream_get["description"]
    assert isinstance(stream_description, str)
    assert "pending" in stream_description, "the stream must name its fallback"

    pending_description = pending_get["description"]
    assert isinstance(pending_description, str)
    assert "stream" in pending_description, "the fallback must name the stream"

    pending_summary = pending_get["summary"]
    assert isinstance(pending_summary, str)
    assert "fallback" in pending_summary
