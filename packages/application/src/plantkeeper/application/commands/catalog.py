"""Catalog commands: asking for a catalogue synchronisation."""

from __future__ import annotations

from plantkeeper.application.commands.base import Command, CommandHandler
from plantkeeper.application.ports.clock import Clock
from plantkeeper.application.ports.unit_of_work import UnitOfWork
from plantkeeper.application.views import SyncRequestedView
from plantkeeper.domain.catalog.events import SpeciesSyncRequested


class RequestSpeciesSyncCommand(Command):
    """Ask for a Trefle synchronisation.

    The command does no synchronisation itself: it appends the
    ``SpeciesSyncRequested`` event that ``SpeciesSyncSaga`` reacts to (Phase 4).
    That is the one event in the catalogue no aggregate raises, because it
    reports no state change — so it goes to the outbox directly.
    """


class RequestSpeciesSyncHandler(CommandHandler[RequestSpeciesSyncCommand, SyncRequestedView]):
    """Append the trigger event to the outbox."""

    def __init__(self, unit_of_work: UnitOfWork, clock: Clock) -> None:
        self._uow = unit_of_work
        self._clock = clock

    async def handle(self, command: RequestSpeciesSyncCommand) -> SyncRequestedView:
        """Publish the request through the same outbox as every other event."""
        async with self._uow:
            await self._uow.outbox.append(SpeciesSyncRequested(occurred_at=self._clock.now()))
            await self._uow.commit()
        return SyncRequestedView(requested=True)
