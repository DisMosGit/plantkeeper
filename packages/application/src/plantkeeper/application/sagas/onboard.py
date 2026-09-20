"""``OnboardPlantSaga``: turn ``PlantAdded`` into a fully set-up plant.

Orchestration, not choreography: one process manager owns the sequence, so the
steps are visible in one place and a failure in a later step rolls back what the
earlier ones created. It is started by ``PlantAdded`` and runs four steps:

1. resolve the species (the ACL the roadmap calls "get ``species_id`` from
   Catalog") and read its watering cadence;
2. create the plant's ``CareSchedule`` — ``CareScheduleCreated``;
3. create the household's first notification — ``NotificationCreated``
   (``plant_onboarded``);
4. publish ``PlantOnboarded``.

Compensation walks that list backwards: the notification is deleted, the schedule
is deleted. ``PlantOnboarded`` is the last step and has no compensation — an
event that was already published cannot be unpublished, so the saga simply does
not reach it when an earlier step fails.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import ClassVar
from uuid import UUID

from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.saga.models import SagaContext
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from pydantic import JsonValue

from plantkeeper.application.errors import NotFoundError, UnhandledSagaTriggerError
from plantkeeper.application.ports.catalog import SpeciesCatalog
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.sagas.base import Saga
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.application.sagas.contexts import OnboardPlantContext
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.garden.events import PlantAdded, PlantOnboarded
from plantkeeper.domain.identifiers import HouseholdId, NotificationId, PlantId, SpeciesId
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.domain.values import WateringInterval


class ResolveSpeciesStep(SagaStepHandler[OnboardPlantContext, None]):
    """Step 1: read the species' watering cadence from the catalogue."""

    def __init__(self, species_catalog: SpeciesCatalog) -> None:
        self._species_catalog = species_catalog

    async def act(self, context: OnboardPlantContext) -> SagaStepResult[OnboardPlantContext, None]:
        """Store the cadence the schedule is built from."""
        species = await self._species_catalog.get(SpeciesId(UUID(context.species_id)))
        if species is None:
            raise NotFoundError(
                f"species {context.species_id} is not in the catalogue, "
                f"so plant {context.plant_id} cannot be onboarded"
            )
        context.watering_interval_seconds = species.watering_interval.value.total_seconds()
        return self._generate_step_result(None)

    async def compensate(self, context: OnboardPlantContext) -> None:
        """Nothing was written; nothing to undo."""


class CreateCareScheduleStep(SagaStepHandler[OnboardPlantContext, None]):
    """Step 2: create the schedule whose first watering is one interval out."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock

    async def act(self, context: OnboardPlantContext) -> SagaStepResult[OnboardPlantContext, None]:
        """Persist the schedule and remember that it exists."""
        now = self._clock.now()
        interval = timedelta(seconds=context.watering_interval_seconds)
        schedule = CareSchedule.create(
            plant_id=PlantId(UUID(context.plant_id)),
            watering_interval=WateringInterval(value=interval),
            starts_at=now + interval,
            now=now,
        )
        await self._unit_of_work.care_schedules.add(schedule)
        await self._unit_of_work.commit()
        context.care_schedule_created = True
        context.next_watering_at = schedule.next_watering_at.isoformat()
        return self._generate_step_result(None)

    async def compensate(self, context: OnboardPlantContext) -> None:
        """Delete the schedule this step created, if it created one."""
        if not context.care_schedule_created:
            return
        await self._unit_of_work.care_schedules.delete(PlantId(UUID(context.plant_id)))
        await self._unit_of_work.commit()
        context.care_schedule_created = False


class CreateOnboardingNotificationStep(SagaStepHandler[OnboardPlantContext, None]):
    """Step 3: create the first reminder for the household."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock

    async def act(self, context: OnboardPlantContext) -> SagaStepResult[OnboardPlantContext, None]:
        """Persist the notification and remember its identifier."""
        payload: dict[str, JsonValue] = {
            "plant_id": context.plant_id,
            "next_watering_at": context.next_watering_at,
        }
        notification = Notification.create(
            household_id=HouseholdId(UUID(context.household_id)),
            notification_type=NotificationType.PLANT_ONBOARDED,
            now=self._clock.now(),
            payload=payload,
        )
        await self._unit_of_work.notifications.add(notification)
        await self._unit_of_work.commit()
        context.notification_id = str(notification.id)
        return self._generate_step_result(None)

    async def compensate(self, context: OnboardPlantContext) -> None:
        """Delete the notification this step created, if it created one."""
        if context.notification_id is None:
            return
        await self._unit_of_work.notifications.delete(NotificationId(UUID(context.notification_id)))
        await self._unit_of_work.commit()
        context.notification_id = None


class PublishPlantOnboardedStep(SagaStepHandler[OnboardPlantContext, None]):
    """Step 4: announce that the plant is set up."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock

    async def act(self, context: OnboardPlantContext) -> SagaStepResult[OnboardPlantContext, None]:
        """Append ``PlantOnboarded`` to the outbox."""
        if context.next_watering_at is None:
            raise NotFoundError(
                f"plant {context.plant_id} has no schedule, so it cannot be onboarded"
            )
        await self._unit_of_work.outbox.append(
            PlantOnboarded(
                plant_id=PlantId(UUID(context.plant_id)),
                household_id=HouseholdId(UUID(context.household_id)),
                species_id=SpeciesId(UUID(context.species_id)),
                next_watering_at=datetime.fromisoformat(context.next_watering_at),
                occurred_at=self._clock.now(),
            )
        )
        await self._unit_of_work.commit()
        return self._generate_step_result(None)

    async def compensate(self, context: OnboardPlantContext) -> None:
        """A published event cannot be unpublished; the last step has no undo."""


class OnboardPlantSaga(Saga):
    """The onboarding process manager, triggered by ``PlantAdded``."""

    trigger_events = (PlantAdded,)
    context_type = OnboardPlantContext
    steps: ClassVar[list[type[SagaStepHandler]]] = [
        ResolveSpeciesStep,
        CreateCareScheduleStep,
        CreateOnboardingNotificationStep,
        PublishPlantOnboardedStep,
    ]

    def context_from_event(self, event: DomainEvent) -> OnboardPlantContext:
        """Start from the three identifiers ``PlantAdded`` already carries."""
        if not isinstance(event, PlantAdded):
            raise UnhandledSagaTriggerError(
                f"OnboardPlantSaga cannot start from {type(event).__name__}"
            )
        return OnboardPlantContext(
            plant_id=str(event.plant_id),
            household_id=str(event.household_id),
            species_id=str(event.species_id),
        )

    def correlation_id(self, context: SagaContext) -> str:
        """A plant has at most one onboarding saga."""
        return self.require_context(context, OnboardPlantContext).plant_id


class OnboardPlantTrigger(Consumer):
    """The consumer group that turns ``PlantAdded`` into an onboarding saga."""

    name = "onboard-plant"
    handled_types = (PlantAdded,)
    saga_name = "OnboardPlantSaga"
    """The saga this trigger starts.

    Declared rather than read off the constructor's annotation: the registry and
    the contract catalogue both need the name without resolving the saga from a
    container.
    """

    def __init__(self, unit_of_work: UnitOfWork, saga: OnboardPlantSaga) -> None:
        super().__init__(unit_of_work)
        self._saga = saga

    async def handle(self, event: DomainEvent, dispatcher: SagaDispatcher) -> None:
        """Dispatch the saga for the newly added plant."""
        await self._saga.handle_event(event, dispatcher=dispatcher, unit_of_work=self.unit_of_work)
