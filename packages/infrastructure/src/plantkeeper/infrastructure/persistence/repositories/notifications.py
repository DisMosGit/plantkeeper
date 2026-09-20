"""SQLAlchemy repository of the Notifications context."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from plantkeeper.domain.identifiers import HouseholdId, NotificationId
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.infrastructure.persistence.mappers.notifications import (
    notification_to_domain,
    notification_to_model,
)
from plantkeeper.infrastructure.persistence.models.notifications import NotificationModel
from plantkeeper.infrastructure.persistence.tracking import AggregateTracker


class SqlAlchemyNotificationRepository:
    """The ``NotificationRepository`` port over SQLAlchemy."""

    def __init__(self, session: AsyncSession, tracker: AggregateTracker) -> None:
        self._session = session
        self._tracker = tracker

    async def add(self, notification: Notification) -> None:
        """Insert a notification."""
        self._session.add(notification_to_model(notification))
        self._tracker.track(notification)

    async def get(self, notification_id: NotificationId) -> Notification | None:
        """Return the notification, or ``None``. Takes no lock."""
        model = await self._session.get(NotificationModel, notification_id.value)
        return None if model is None else notification_to_domain(model)

    async def get_for_update(self, notification_id: NotificationId) -> Notification | None:
        """Return the notification, locking it for this transaction."""
        statement = (
            select(NotificationModel)
            .where(NotificationModel.id == notification_id.value)
            .with_for_update()
        )
        model = (await self._session.execute(statement)).scalar_one_or_none()
        return None if model is None else notification_to_domain(model)

    async def save(self, notification: Notification) -> None:
        """Persist the notification's current state."""
        await self._session.merge(notification_to_model(notification))
        self._tracker.track(notification)

    async def delete(self, notification_id: NotificationId) -> None:
        """Remove the notification, if it exists."""
        model = await self._session.get(NotificationModel, notification_id.value)
        if model is not None:
            await self._session.delete(model)

    async def list_pending(self, household_id: HouseholdId) -> list[Notification]:
        """List the household's unacknowledged notifications, oldest first."""
        statement = (
            select(NotificationModel)
            .where(
                NotificationModel.household_id == household_id.value,
                NotificationModel.read_at.is_(None),
            )
            .order_by(NotificationModel.created_at, NotificationModel.id)
        )
        models = (await self._session.execute(statement)).scalars()
        return [notification_to_domain(model) for model in models]
