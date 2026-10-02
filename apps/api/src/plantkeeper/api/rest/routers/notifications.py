"""Notification delivery endpoints: an SSE stream, with request and wait as fallback.

``GET /stream`` is the primary form. It is woken by the same payload-free
household signal the request/wait endpoint waits on, re-reads the write tables on
every nudge, and resumes from a cursor, so a reconnecting client receives what it
missed and nothing it already saw. While it idles it holds a channel subscription
and no database session.

``GET /pending`` is the fallback and keeps its behaviour: it answers immediately
when something is pending, opens the household's presence channel and waits when
nothing is, re-reads after every wake and once more at the deadline, and answers
``204 No Content`` to "nothing appeared". ``POST /{id}/ack`` is how a delivered
notification is consumed, from either form.

The wait is a *nudge*, not the delivery: the channel carries no payload, and every
answer comes from the write tables. That is what makes a missed signal a delay
instead of a lost notification, and why both forms re-read after a wake and once
more at the deadline.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Annotated
from uuid import UUID

from cqrs.mediator import RequestMediator
from fastapi import APIRouter, Header, Query, Request, Response
from fastapi.responses import StreamingResponse

from plantkeeper.api.deps import Mediator, NotificationChannelDep, NotificationStreamDep
from plantkeeper.api.mediator import view_of
from plantkeeper.api.rest import sse
from plantkeeper.api.rest.schemas.notifications import (
    NotificationCollectionResponse,
    NotificationResponse,
)
from plantkeeper.application.commands.notifications import AcknowledgeNotificationCommand
from plantkeeper.application.notifications.stream import (
    NotificationArrival,
    NotificationStreamEvent,
    StreamKeepAlive,
    StreamOpened,
)
from plantkeeper.application.ports.notifications import NotificationChannelError
from plantkeeper.application.queries.notifications import ListPendingNotificationsQuery
from plantkeeper.application.views import CollectionView, NotificationView
from plantkeeper.domain.identifiers import HouseholdId, NotificationId

router = APIRouter(prefix="/api/v1/notifications", tags=["notifications"])

logger = logging.getLogger(__name__)

DEFAULT_POLL_TIMEOUT_SECONDS: float = 0.0
"""How long a caller waits when it does not ask.

Zero, so a plain poll still answers immediately: long polling is something a
client opts into with ``timeout=30``, and a caller that omitted the parameter must
not be held for half a minute.
"""

MAX_POLL_TIMEOUT_SECONDS: float = 60.0
"""The longest wait a caller may ask for.

A long poll holds a database connection and a Valkey subscription for its whole
duration, so the wait is capped rather than left to the client.
"""


async def _pending(
    mediator: RequestMediator, household_id: HouseholdId
) -> CollectionView[NotificationView]:
    """Read the household's unacknowledged notifications through the mediator."""
    return view_of(
        await mediator.send(ListPendingNotificationsQuery(household_id=household_id)),
        CollectionView[NotificationView],
    )


@router.get(
    "/pending",
    response_model=NotificationCollectionResponse,
    responses={204: {"description": "No notification appeared before the timeout."}},
    summary="Wait for the household's pending notifications (fallback)",
    description=(
        "The request-and-wait fallback to `GET /api/v1/notifications/stream`: "
        "answers immediately with the household's unread notifications when there "
        "are any, otherwise waits on the household signal for up to `timeout` "
        "seconds and answers `204` when nothing appeared. One request-scoped "
        "database session and one channel subscription are held for the whole "
        "wait, which is why the stream is the primary form."
    ),
)
async def list_pending(
    mediator: Mediator,
    channel: NotificationChannelDep,
    household_id: Annotated[UUID, Query(description="The household whose notifications to list.")],
    timeout: Annotated[
        float,
        Query(
            ge=0.0,
            le=MAX_POLL_TIMEOUT_SECONDS,
            description="Seconds to wait for a notification when there are none yet.",
        ),
    ] = DEFAULT_POLL_TIMEOUT_SECONDS,
) -> NotificationCollectionResponse | Response:
    """Return the household's unacknowledged notifications, waiting if there are none.

    ``200`` with the pending notifications, or ``204`` when none appeared within
    ``timeout``. A ``timeout`` of zero answers immediately.
    """
    household = HouseholdId(household_id)
    pending = await _pending(mediator, household)
    if pending.items:
        return NotificationCollectionResponse.from_view(pending)
    if timeout <= 0:
        return Response(status_code=204)

    try:
        async with channel.subscribe(household) as subscription:
            # Subscribed before re-reading: a notification created between the
            # first read and the subscription is caught here, and one created
            # after it signals the channel.
            pending = await _pending(mediator, household)
            if pending.items:
                return NotificationCollectionResponse.from_view(pending)
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout
            while (remaining := deadline - loop.time()) > 0:
                if not await subscription.wait(remaining):
                    break
                pending = await _pending(mediator, household)
                if pending.items:
                    return NotificationCollectionResponse.from_view(pending)
    except NotificationChannelError as exc:
        # The channel is gone, but the notifications are not: answer from the
        # database rather than failing a request that can be served.
        logger.warning("notification channel unavailable, answering without waiting: %s", exc)

    pending = await _pending(mediator, household)
    if pending.items:
        return NotificationCollectionResponse.from_view(pending)
    return Response(status_code=204)


def _cursor(since: UUID | None, last_event_id: UUID | None) -> NotificationId | None:
    """Resolve the resume cursor: an explicit ``since`` wins over SSE's header."""
    raw = since if since is not None else last_event_id
    return None if raw is None else NotificationId(raw)


async def _frames(
    request: Request, events: AsyncGenerator[NotificationStreamEvent]
) -> AsyncIterator[str]:
    """Translate the application's stream into SSE frames.

    The retry hint is written first, before the stream yields anything, so a
    client knows its reconnection delay even when the signal is unavailable and
    the response is a plain read. Frames stop being written as soon as the client
    is gone: a stream is long-lived, and a closed connection must not keep its
    share of the household's subscription.
    """
    yield sse.sse_retry(sse.RETRY_MILLISECONDS)
    try:
        async for event in events:
            if await request.is_disconnected():
                break
            match event:
                case StreamOpened(degraded=True):
                    yield sse.sse_comment("signal unavailable: answering from storage")
                case StreamOpened():
                    yield sse.sse_comment("stream open")
                case StreamKeepAlive():
                    yield sse.sse_comment("keep-alive")
                case NotificationArrival(notification=notification):
                    yield sse.sse_event(
                        data=NotificationResponse.from_view(notification).model_dump_json(),
                        event="notification",
                        event_id=str(notification.notification_id.value),
                    )
    finally:
        await events.aclose()


@router.get(
    "/stream",
    response_class=StreamingResponse,
    summary="Stream the household's notifications",
    description=(
        "The primary delivery form: a `text/event-stream` that stays open and "
        "pushes each unread notification as it appears, woken by the household "
        "signal and answered from the application's own tables. Resumes from a "
        "cursor — `since`, or the `Last-Event-ID` header an SSE client reconnects "
        "with, where an explicit `since` wins — so a reconnect receives what it "
        "missed and nothing it already saw. Comment lines keep an idle stream "
        "alive and `retry` states the reconnect delay; an idle stream holds no "
        "database session. When the signal is unavailable the stream degrades to a "
        "plain read: the pending notifications, then the end of the response. "
        "`GET /api/v1/notifications/pending` remains the request-and-wait fallback."
    ),
    responses={
        200: {
            "description": (
                "An open SSE stream. `event: notification` frames carry the same "
                "payload as an item of `/pending`; comment lines are keep-alives, "
                "and the first one says whether the household signal was reached."
            ),
            "content": {"text/event-stream": {"schema": {"type": "string"}}},
        },
        422: {"description": "A parameter is missing or is not a UUID."},
    },
)
async def stream_notifications(
    request: Request,
    stream: NotificationStreamDep,
    household_id: Annotated[
        UUID, Query(description="The household whose notifications to stream.")
    ],
    since: Annotated[
        UUID | None,
        Query(
            description=(
                "The last notification the client saw; omit it to receive everything "
                "unread. The `Last-Event-ID` header is the same cursor."
            )
        ),
    ] = None,
    last_event_id: Annotated[
        UUID | None,
        Header(description="The SSE reconnect cursor, sent automatically after `id:` frames."),
    ] = None,
) -> StreamingResponse:
    """Stream the household's unread notifications as server-sent events.

    Never waits before answering: the response opens as soon as the household's
    signal is attached (or found to be unavailable), and each notification is a
    frame of its own.
    """
    events = stream.stream(HouseholdId(household_id), since=_cursor(since, last_event_id))
    return StreamingResponse(
        _frames(request, events),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # A buffering proxy would hold every frame until the stream ended.
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/{notification_id}/ack", response_model=NotificationResponse, summary="Acknowledge")
async def acknowledge(notification_id: UUID, mediator: Mediator) -> NotificationResponse:
    """Acknowledge a notification.

    Returns 409 when it was already acknowledged: that is the aggregate's rule,
    and answering 200 twice would report a state change that did not happen.
    """
    command = AcknowledgeNotificationCommand(notification_id=NotificationId(notification_id))
    return NotificationResponse.from_view(view_of(await mediator.send(command), NotificationView))
