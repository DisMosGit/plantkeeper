"""Notifications queries."""

from __future__ import annotations

from plantkeeper.application.ports.repositories import NotificationRepository
from plantkeeper.application.queries.base import Query, QueryHandler
from plantkeeper.application.views import CollectionView, NotificationView
from plantkeeper.domain.identifiers import HouseholdId, NotificationId


class ListPendingNotificationsQuery(Query):
    """Read a household's unacknowledged notifications."""

    household_id: HouseholdId
    since: NotificationId | None = None
    """Return only what comes after this notification.

    The request-and-wait endpoint asks without a cursor; a stream passes the
    last notification it delivered and re-asks after every wake-up, which is how
    it resumes rather than repeats.
    """


class ListPendingNotificationsHandler(
    QueryHandler[ListPendingNotificationsQuery, CollectionView[NotificationView]]
):
    """Answer with the pending notifications, oldest first.

    One handler answers both delivery forms: the request-and-wait endpoint reads
    it once per request, and the stream reads it again after every nudge, with
    the cursor it has advanced to.
    """

    def __init__(self, notifications: NotificationRepository) -> None:
        self._notifications = notifications

    async def handle(
        self, query: ListPendingNotificationsQuery
    ) -> CollectionView[NotificationView]:
        """Return the pending notifications of the household."""
        notifications = await self._notifications.list_pending(
            query.household_id, since=query.since
        )
        return CollectionView[NotificationView](
            items=[NotificationView.from_domain(n) for n in notifications]
        )
