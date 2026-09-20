"""Notifications commands: acknowledging a notification."""

from __future__ import annotations

from plantkeeper.application.commands.base import Command, CommandHandler
from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.views import NotificationView
from plantkeeper.domain.identifiers import NotificationId


class AcknowledgeNotificationCommand(Command):
    """Acknowledge one notification."""

    notification_id: NotificationId


class AcknowledgeNotificationHandler(
    CommandHandler[AcknowledgeNotificationCommand, NotificationView]
):
    """Set ``read_at`` and record ``NotificationRead``."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: AcknowledgeNotificationCommand) -> NotificationView:
        """Acknowledge the notification, or fail because it was already read."""
        async with self._uow:
            notification = await self._uow.notifications.get_for_update(command.notification_id)
            if notification is None:
                raise NotFoundError(f"notification {command.notification_id} does not exist")
            notification.mark_read(now=self._clock.now())
            await self._uow.notifications.save(notification)
            await self._uow.commit()
        return NotificationView.from_domain(notification)
