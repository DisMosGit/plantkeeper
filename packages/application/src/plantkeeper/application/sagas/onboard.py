"""``OnboardPlantSaga``: turn ``PlantAdded`` into a fully set-up plant.

Orchestration, not choreography: one process manager owns the sequence, so the
steps are visible in one place and a failure in a later step rolls back what the
earlier ones created. It is started by ``PlantAdded`` and runs four steps:

1. resolve the species (the catalogue ACL's "get ``species_id`` from Catalog")
   and read its watering cadence;
2. **record** ``CreateCareScheduleCommand`` for the care context;
3. **record** ``CreateOnboardingNotificationCommand`` for the notifications
   context;
4. append ``PlantOnboarded`` to the outbox.

Steps 2 and 3 do not write ``write_care`` or ``write_notifications``: a process
manager that wrote another context's tables would be a second writer of them, which
is the coupling ``event-transport`` forbids. They record a command in the very
transaction that checkpoints the process, and the command dispatcher executes it
through the owning context's own handler and unit of work
(``docs/adr/0012-saga-command-dispatch.md``).

Compensation walks that list backwards. For steps 3 and 2 it *cancels* the recorded
command when the dispatcher has not run it yet — the effect then never happens, so
there is nothing to undo — and records the matching delete command when it has.
``PlantOnboarded`` is the last step and has no compensation: an event that was
already published cannot be unpublished, so the saga simply does not reach it when
an earlier step fails.
"""

from __future__ import annotations

from datetime import timedelta
from typing import ClassVar
from uuid import UUID, uuid5

from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.saga.models import SagaContext
from cqrs.saga.step import SagaStepHandler, SagaStepResult

from plantkeeper.application.commands.care import (
    CreateCareScheduleCommand,
    DeleteCareScheduleCommand,
)
from plantkeeper.application.commands.notifications import (
    CreateOnboardingNotificationCommand,
    DeleteNotificationCommand,
)
from plantkeeper.application.errors import NotFoundError, UnhandledSagaTriggerError
from plantkeeper.application.ports.catalog import SpeciesCatalog
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.saga_intents import recorded_intent
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.sagas.base import Saga
from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.application.sagas.contexts import OnboardPlantContext
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.garden.events import PlantAdded, PlantOnboarded
from plantkeeper.domain.identifiers import HouseholdId, NotificationId, PlantId, SpeciesId

STEP_CARE_SCHEDULE = 2
"""The step number recorded with the care context's command.

Declared as constants because they are part of the intent's identity: the
idempotency key is derived from ``(saga id, step number)``, so renumbering the steps
would change what a re-execution deduplicates against.
"""

STEP_NOTIFICATION = 3


class ResolveSpeciesStep(SagaStepHandler[OnboardPlantContext, None]):
    """Step 1: read the species' watering cadence from the catalogue."""

    def __init__(self, species_catalog: SpeciesCatalog) -> None:
        self._species_catalog = species_catalog

    async def act(self, context: OnboardPlantContext) -> SagaStepResult[OnboardPlantContext, None]:
        """Store the cadence the schedule is built from.

        A *read* of the catalogue through this saga's ACL port, not a write: the
        context that owns species owns the table, and the onboarding process is
        allowed to ask it a question.
        """
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
    """Step 2: record the care context's command to create the schedule."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock

    async def act(self, context: OnboardPlantContext) -> SagaStepResult[OnboardPlantContext, None]:
        """Record the command, in this step's own transaction."""
        command = CreateCareScheduleCommand(
            plant_id=PlantId(UUID(context.plant_id)),
            watering_interval_seconds=context.watering_interval_seconds,
        )
        await self._unit_of_work.saga_intents.record(
            recorded_intent(
                saga_id=UUID(context.saga_id),
                step_no=STEP_CARE_SCHEDULE,
                command_name=type(command).__name__,
                payload=command.model_dump(mode="json"),
            )
        )
        context.care_schedule_created = True
        await self._unit_of_work.commit()
        return self._generate_step_result(None)

    async def compensate(self, context: OnboardPlantContext) -> None:
        """Withdraw the command, or record its undo if it already ran."""
        if not context.care_schedule_created:
            return
        await withdraw(
            unit_of_work=self._unit_of_work,
            saga_id=UUID(context.saga_id),
            step_no=STEP_CARE_SCHEDULE,
            undo=DeleteCareScheduleCommand(plant_id=PlantId(UUID(context.plant_id))),
        )
        context.care_schedule_created = False


class CreateOnboardingNotificationStep(SagaStepHandler[OnboardPlantContext, None]):
    """Step 3: record the notifications context's command for the first reminder."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock

    async def act(self, context: OnboardPlantContext) -> SagaStepResult[OnboardPlantContext, None]:
        """Mint the reminder's identity and record the command.

        The identifier is minted *here* and travels in the payload, which is what
        makes the command reproducible: the dispatcher may execute the same recorded
        command more than once, and a re-run that minted a fresh identifier would
        look like a different request rather than a replay. It is also what lets the
        compensation name the row it has to undo.
        """
        notification_id = _reminder_id(UUID(context.saga_id), STEP_NOTIFICATION)
        command = CreateOnboardingNotificationCommand(
            household_id=HouseholdId(UUID(context.household_id)),
            plant_id=PlantId(UUID(context.plant_id)),
            next_watering_at=self._clock.now()
            + timedelta(seconds=context.watering_interval_seconds),
            notification_id=notification_id,
        )
        await self._unit_of_work.saga_intents.record(
            recorded_intent(
                saga_id=UUID(context.saga_id),
                step_no=STEP_NOTIFICATION,
                command_name=type(command).__name__,
                payload=command.model_dump(mode="json"),
            )
        )
        context.notification_id = str(notification_id)
        await self._unit_of_work.commit()
        return self._generate_step_result(None)

    async def compensate(self, context: OnboardPlantContext) -> None:
        """Withdraw the command, or record its undo if it already ran."""
        if context.notification_id is None:
            return
        await withdraw(
            unit_of_work=self._unit_of_work,
            saga_id=UUID(context.saga_id),
            step_no=STEP_NOTIFICATION,
            undo=DeleteNotificationCommand(
                notification_id=NotificationId(UUID(context.notification_id))
            ),
        )
        context.notification_id = None


class PublishPlantOnboardedStep(SagaStepHandler[OnboardPlantContext, None]):
    """Step 4: announce that the plant is set up.

    Last on purpose. It is the one step with no undo, so it must not run until the
    delegated work has been recorded — and recording is all the earlier steps do,
    which is why this step no longer waits on a schedule that exists.
    """

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock

    async def act(self, context: OnboardPlantContext) -> SagaStepResult[OnboardPlantContext, None]:
        """Append ``PlantOnboarded`` to the outbox."""
        if not context.care_schedule_created:
            raise NotFoundError(
                f"plant {context.plant_id} has no recorded schedule command, "
                f"so it cannot be onboarded"
            )
        await self._unit_of_work.outbox.append(
            PlantOnboarded(
                plant_id=PlantId(UUID(context.plant_id)),
                household_id=HouseholdId(UUID(context.household_id)),
                species_id=SpeciesId(UUID(context.species_id)),
                next_watering_at=self._clock.now()
                + timedelta(seconds=context.watering_interval_seconds),
                occurred_at=self._clock.now(),
            )
        )
        await self._unit_of_work.commit()
        return self._generate_step_result(None)

    async def compensate(self, context: OnboardPlantContext) -> None:
        """A published event cannot be unpublished; the last step has no undo."""


async def withdraw(
    *,
    unit_of_work: UnitOfWork,
    saga_id: UUID,
    step_no: int,
    undo: CreateCareScheduleCommand | DeleteCareScheduleCommand | DeleteNotificationCommand,
) -> None:
    """Cancel a recorded command, or record the command that undoes it.

    Cancelling is preferred: an intent that has not been dispatched yet can simply
    be withdrawn, so the effect never happens and no compensation is needed. Once
    the dispatcher has run it the effect exists, and the only honest rollback is
    another recorded command — issued to the same context, through the same
    hand-off, so the saga still never writes a table it does not own.
    """
    cancelled = await unit_of_work.saga_intents.cancel_pending(
        recorded_intent(
            saga_id=saga_id,
            step_no=step_no,
            command_name="",
            payload={},
        ).idempotency_key
    )
    if cancelled:
        await unit_of_work.commit()
        return
    await unit_of_work.saga_intents.record(
        recorded_intent(
            saga_id=saga_id,
            step_no=step_no + COMPENSATION_STEP_OFFSET,
            command_name=type(undo).__name__,
            payload=undo.model_dump(mode="json"),
        )
    )
    await unit_of_work.commit()


def _reminder_id(saga_id: UUID, step_no: int) -> NotificationId:
    """Derive the reminder's identifier from the intent that asks for it.

    Deterministic rather than random, for the reason the step's docstring gives: a
    recorded command has to be reproducible to be recognised as a replay.
    """
    return NotificationId(uuid5(_INTENT_NAMESPACE, f"{saga_id}:{step_no}"))


_INTENT_NAMESPACE = UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")
"""A fixed namespace for identifiers derived from a recorded command.

Any constant would do; this one is written out rather than generated so the same
intent yields the same identifier on every process that ever computes it.
"""


COMPENSATION_STEP_OFFSET = 1000
"""Added to a step number to key the command that undoes it.

Keeps the two commands for one step distinct while deriving both keys from the same
``(saga id, step number)`` identity, so a compensation delivered twice still
deduplicates against itself.
"""


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
        """Start from the three identifiers ``PlantAdded`` already carries.

        The saga id is filled in by :meth:`Saga.handle_event` once it has derived
        it; a step records it with the command it delegates so the dispatcher can
        attribute the intent to the process that asked for it.
        """
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
