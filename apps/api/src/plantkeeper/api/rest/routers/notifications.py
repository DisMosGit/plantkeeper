"""Notification endpoints.

Phase 8 replaces the read endpoint with an HTTP long poll that waits for a
notification instead of answering an empty list; the URL shape is already its
shape, so that change will not move a client.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from plantkeeper.api.deps import Mediator, view_of
from plantkeeper.api.rest.schemas.notifications import (
    NotificationCollectionResponse,
    NotificationResponse,
)
from plantkeeper.application.commands.notifications import AcknowledgeNotificationCommand
from plantkeeper.application.queries.notifications import ListPendingNotificationsQuery
from plantkeeper.application.views import CollectionView, NotificationView
from plantkeeper.domain.identifiers import HouseholdId, NotificationId

router = APIRouter(prefix="/api/v1/notifications", tags=["notifications"])


@router.get(
    "/pending",
    response_model=NotificationCollectionResponse,
    summary="List pending notifications",
)
async def list_pending(
    mediator: Mediator,
    household_id: Annotated[UUID, Query(description="The household whose notifications to list.")],
) -> NotificationCollectionResponse:
    """Return the household's unacknowledged notifications, oldest first."""
    query = ListPendingNotificationsQuery(household_id=HouseholdId(household_id))
    view = view_of(await mediator.send(query), CollectionView[NotificationView])
    return NotificationCollectionResponse.from_view(view)


@router.post("/{notification_id}/ack", response_model=NotificationResponse, summary="Acknowledge")
async def acknowledge(notification_id: UUID, mediator: Mediator) -> NotificationResponse:
    """Acknowledge a notification.

    Returns 409 when it was already acknowledged: that is the aggregate's rule,
    and answering 200 twice would report a state change that did not happen.
    """
    command = AcknowledgeNotificationCommand(notification_id=NotificationId(notification_id))
    return NotificationResponse.from_view(view_of(await mediator.send(command), NotificationView))
