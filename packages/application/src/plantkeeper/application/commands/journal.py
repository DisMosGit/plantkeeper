"""Journal commands: appending a care entry to a plant's event stream."""

from __future__ import annotations

from pydantic import AwareDatetime

from plantkeeper.application.commands.base import Command, CommandHandler
from plantkeeper.application.errors import NotFoundError
from plantkeeper.application.journal.store import record_journal_entry
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.views import JournalEntryView
from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.values import JournalEntryType


class AddJournalEntryCommand(Command):
    """Append an entry to a plant's journal.

    ``occurred_at`` is when the care happened; it defaults to the wall clock, and a
    caller that knows better — a consumer replaying ``WateringCompleted`` — supplies
    the event's own moment so the journal and the care history agree.
    """

    plant_id: PlantId
    entry_type: JournalEntryType
    occurred_at: AwareDatetime | None = None
    note: str | None = None
    entry_id: JournalEntryId | None = None


class AddJournalEntryHandler(CommandHandler[AddJournalEntryCommand, JournalEntryView]):
    """Append one immutable entry, in its own transaction.

    The entry is a fact of the Journal, not of the Garden, so the only claim made
    about the plant is that it exists: journalling a plant that was never added would
    create a stream nothing can ever show.
    """

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: AddJournalEntryCommand) -> JournalEntryView:
        """Append the entry and answer with it."""
        now = self._clock.now()
        async with self._uow:
            plant = await self._uow.plants.get(command.plant_id)
            if plant is None:
                raise NotFoundError(f"plant {command.plant_id} does not exist")
            entry = await record_journal_entry(
                self._uow,
                plant_id=command.plant_id,
                entry_type=command.entry_type,
                occurred_at=command.occurred_at if command.occurred_at is not None else now,
                now=now,
                note=command.note,
                entry_id=command.entry_id,
            )
            await self._uow.commit()
        return JournalEntryView.from_domain(entry)
