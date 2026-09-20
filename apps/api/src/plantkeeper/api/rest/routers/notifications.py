"""Notification endpoints: HTTP long polling (Phase 8).

``GET /pending`` answers with the household's unacknowledged notifications. When
there are none it does not answer an empty list immediately: it opens the
household's presence channel and waits, re-reading its own database every time the
channel signals, until a notification appears or the caller's ``timeout`` runs out.
``204 No Content`` is the answer to "nothing appeared", and ``POST /{id}/ack`` is
how a delivered notification is consumed.

The wait is a *nudge*, not the delivery: the channel carries no payload, and every
answer comes from the write tables. That is what makes a missed signal a delay
instead of a lost notification, and why the endpoint re-reads after a wake and once
more at the deadline.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated
from uuid import UUID

from cqrs.mediator import RequestMediator
from fastapi import APIRouter, Query, Response

from plantkeeper.api.deps import Mediator, NotificationChannelDep
from plantkeeper.api.mediator import view_of
from plantkeeper.api.rest.schemas.notifications import (
    NotificationCollectionResponse,
    NotificationResponse,
)
from plantkeeper.application.commands.notifications import AcknowledgeNotificationCommand
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
    summary="Wait for the household's pending notifications",
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


@router.post("/{notification_id}/ack", response_model=NotificationResponse, summary="Acknowledge")
async def acknowledge(notification_id: UUID, mediator: Mediator) -> NotificationResponse:
    """Acknowledge a notification.

    Returns 409 when it was already acknowledged: that is the aggregate's rule,
    and answering 200 twice would report a state change that did not happen.
    """
    command = AcknowledgeNotificationCommand(notification_id=NotificationId(notification_id))
    return NotificationResponse.from_view(view_of(await mediator.send(command), NotificationView))
