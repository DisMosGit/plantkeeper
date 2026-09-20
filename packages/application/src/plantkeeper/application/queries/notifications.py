"""Notifications queries."""

from __future__ import annotations

from plantkeeper.application.ports.repositories import NotificationRepository
from plantkeeper.application.queries.base import Query, QueryHandler
from plantkeeper.application.views import CollectionView, NotificationView
from plantkeeper.domain.identifiers import HouseholdId


class ListPendingNotificationsQuery(Query):
    """Read a household's unacknowledged notifications."""

    household_id: HouseholdId


class ListPendingNotificationsHandler(
    QueryHandler[ListPendingNotificationsQuery, CollectionView[NotificationView]]
):
    """Answer with the pending notifications, oldest first.

    Phase 8 replaces this with an HTTP long poll that waits for a notification
    instead of returning an empty list; the read model behind it is the same.
    """

    def __init__(self, notifications: NotificationRepository) -> None:
        self._notifications = notifications

    async def handle(
        self, query: ListPendingNotificationsQuery
    ) -> CollectionView[NotificationView]:
        """Return the pending notifications of the household."""
        notifications = await self._notifications.list_pending(query.household_id)
        return CollectionView[NotificationView](
            items=[NotificationView.from_domain(n) for n in notifications]
        )
