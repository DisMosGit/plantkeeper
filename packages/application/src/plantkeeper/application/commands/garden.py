"""Garden commands: households and the plants they own."""

from __future__ import annotations

from plantkeeper.application.commands.base import Command, CommandHandler, IdempotentCommand
from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.idempotency import commit_create
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.views import HouseholdView, PlantView
from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SpeciesId
from plantkeeper.domain.values import Location

CREATED = 201
"""The status a create use case answers with on the first and on a replayed call."""


class CreateHouseholdCommand(IdempotentCommand):
    """Create a household. Replayable through ``Idempotency-Key``."""

    name: str


class CreateHouseholdHandler(CommandHandler[CreateHouseholdCommand, HouseholdView]):
    """Create a household aggregate and persist it."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: CreateHouseholdCommand) -> HouseholdView:
        """Create the household once, however often the client retries."""
        return await commit_create(
            uow=self._uow,
            clock=self._clock,
            command=command,
            view_type=HouseholdView,
            status_code=CREATED,
            operation=lambda: self._create(command),
        )

    async def _create(self, command: CreateHouseholdCommand) -> HouseholdView:
        household = Household.create(name=command.name)
        await self._uow.households.add(household)
        return HouseholdView.from_domain(household)


class AddPlantCommand(IdempotentCommand):
    """Add a plant to a household. Replayable through ``Idempotency-Key``."""

    household_id: HouseholdId
    species_id: SpeciesId
    name: str
    location: Location


class AddPlantHandler(CommandHandler[AddPlantCommand, PlantView]):
    """Create the plant and register it with its household.

    The household is loaded first and locked, so the "at most 50 plants" rule
    holds even when two plants are added to the same household at once: the
    membership the rule reads is derived from the plants table, and the lock
    serialises the two transactions.
    """

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: AddPlantCommand) -> PlantView:
        """Add the plant once, however often the client retries."""
        return await commit_create(
            uow=self._uow,
            clock=self._clock,
            command=command,
            view_type=PlantView,
            status_code=CREATED,
            operation=lambda: self._add(command),
        )

    async def _add(self, command: AddPlantCommand) -> PlantView:
        household = await self._uow.households.get_for_update(command.household_id)
        if household is None:
            raise NotFoundError(f"household {command.household_id} does not exist")

        plant = Plant.add(
            household_id=command.household_id,
            species_id=command.species_id,
            name=command.name,
            location=command.location,
            now=self._clock.now(),
        )
        # Registers the plant in the aggregate and enforces the cap. The
        # membership itself is persisted by the plant row, not by the household.
        household.add_plant(plant.id)

        await self._uow.plants.add(plant)
        return PlantView.from_domain(plant)


class RemovePlantCommand(Command):
    """Remove a plant from its household."""

    plant_id: PlantId


class RemovePlantHandler(CommandHandler[RemovePlantCommand, PlantView]):
    """Mark the plant removed and publish ``PlantRemoved``."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: RemovePlantCommand) -> PlantView:
        """Remove the plant, or fail because it is already gone."""
        async with self._uow:
            plant = await self._uow.plants.get(command.plant_id)
            if plant is None:
                raise NotFoundError(f"plant {command.plant_id} does not exist")
            plant.remove(now=self._clock.now())
            await self._uow.plants.save(plant)
            await self._uow.commit()
        return PlantView.from_domain(plant)


class MovePlantCommand(Command):
    """Move a plant to another location."""

    plant_id: PlantId
    location: Location


class MovePlantHandler(CommandHandler[MovePlantCommand, PlantView]):
    """Change the location, recording ``PlantMoved`` only if it really changed."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: MovePlantCommand) -> PlantView:
        """Move the plant, or return it unchanged when it is already there."""
        async with self._uow:
            plant = await self._uow.plants.get(command.plant_id)
            if plant is None:
                raise NotFoundError(f"plant {command.plant_id} does not exist")
            plant.move(command.location, now=self._clock.now())
            await self._uow.plants.save(plant)
            await self._uow.commit()
        return PlantView.from_domain(plant)
