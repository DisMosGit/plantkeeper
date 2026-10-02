"""Notifications commands: creating the onboarding reminder, acknowledging one."""

from __future__ import annotations

from pydantic import AwareDatetime, JsonValue

from plantkeeper.application.commands.base import Command, CommandHandler, IdempotentCommand
from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.idempotency import CREATED, commit_create
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.views import NotificationView
from plantkeeper.domain.identifiers import HouseholdId, NotificationId, PlantId
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType


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


class CreateOnboardingNotificationCommand(IdempotentCommand):
    """Create the household's "plant onboarded" reminder.

    Recorded by ``OnboardPlantSaga`` rather than written by it: ``write_notifications``
    belongs to the notifications context, so the saga records the command and this
    context's own unit of work creates the row
    (``docs/adr/0012-saga-command-dispatch.md``).
    """

    household_id: HouseholdId
    plant_id: PlantId
    next_watering_at: AwareDatetime
    notification_id: NotificationId | None = None
    """The identifier to create the reminder under.

    Supplied by the recording step and therefore **stable across executions**: the
    dispatcher may run the same recorded command more than once, and a re-run has
    to produce the same command to be recognised as a replay rather than a second
    request. Left ``None`` for a caller that just wants a reminder now.
    """


class CreateOnboardingNotificationHandler(
    CommandHandler[CreateOnboardingNotificationCommand, NotificationView]
):
    """Create the reminder and record ``NotificationCreated`` in one transaction."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: CreateOnboardingNotificationCommand) -> NotificationView:
        """Create the reminder once, however often the command is delivered."""
        return await commit_create(
            uow=self._uow,
            clock=self._clock,
            command=command,
            view_type=NotificationView,
            status_code=CREATED,
            operation=lambda: self._create(command),
        )

    async def _create(self, command: CreateOnboardingNotificationCommand) -> NotificationView:
        payload: dict[str, JsonValue] = {
            "plant_id": str(command.plant_id),
            "next_watering_at": command.next_watering_at.isoformat(),
        }
        notification = Notification.create(
            household_id=command.household_id,
            notification_type=NotificationType.PLANT_ONBOARDED,
            now=self._clock.now(),
            payload=payload,
            notification_id=command.notification_id,
        )
        await self._uow.notifications.add(notification)
        return NotificationView.from_domain(notification)


class DeleteNotificationCommand(IdempotentCommand):
    """Remove one notification.

    The undo half of :class:`CreateOnboardingNotificationCommand`; see
    :class:`~plantkeeper.application.commands.care.DeleteCareScheduleCommand` for
    why the rollback is recorded rather than performed by the saga.
    """

    notification_id: NotificationId


class DeleteNotificationHandler(CommandHandler[DeleteNotificationCommand, NotificationView | None]):
    """Delete the notification, answering ``None`` when it was already gone."""

    def __init__(self, unit_of_work: UnitOfWork) -> None:
        self._uow = unit_of_work

    async def handle(self, command: DeleteNotificationCommand) -> NotificationView | None:
        """Delete the notification if it exists, in one transaction."""
        async with self._uow:
            notification = await self._uow.notifications.get_for_update(command.notification_id)
            if notification is None:
                return None
            await self._uow.notifications.delete(command.notification_id)
            await self._uow.commit()
        return NotificationView.from_domain(notification)
