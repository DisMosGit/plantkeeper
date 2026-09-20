"""The Journal's event store.

The journal is the project's one event-sourced aggregate (``docs/event-sourcing.md``),
so this port is shaped by what replaying it needs:

* ``append`` writes facts to a stream at known versions. The stream id is the plant
  id, and ``(stream_id, version)`` is the optimistic-locking token: two writers
  that both read version N cannot both append N+1.
* ``load_stream`` reads one stream in version order, optionally *after* a version,
  so a snapshot can stand in for the events it already covers.
* ``load_all`` reads across streams in global append order. Nothing on the write
  path needs it yet; it is the hook a replay or rebuild tool uses, and it is
  tested so it does not rot.

``StoredEvent`` is a frozen dataclass rather than a Pydantic model on purpose: it
carries a domain event, and a Pydantic field typed ``DomainEvent`` would flatten
the subclass on validation. It is an internal read result, not a payload crossing a
Kafka or gRPC boundary, so a dataclass is allowed here.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.identifiers import PlantId
from plantkeeper.domain.journal.state import JournalState


@dataclass(frozen=True, slots=True)
class StoredEvent:
    """One stored event, and where it sits in its stream and in the store."""

    stream_id: PlantId
    version: int
    global_position: int
    event: DomainEvent


@runtime_checkable
class EventStoreRepository(Protocol):
    """Append-only access to the event store."""

    async def append(
        self,
        stream_id: PlantId,
        *,
        expected_version: int,
        events: Sequence[DomainEvent],
    ) -> None:
        """Append ``events`` at ``expected_version + 1`` onwards.

        Raises ``EventStoreConcurrencyError`` when the stream already holds one of
        those versions: another writer got there first, which is what the optimistic
        lock exists to detect.
        """
        ...

    async def load_stream(self, stream_id: PlantId, *, after_version: int = 0) -> list[StoredEvent]:
        """Read a stream's events strictly after ``after_version``, oldest first."""
        ...

    async def load_all(self, *, after_position: int = 0, limit: int = 1000) -> list[StoredEvent]:
        """Read events across every stream in global append order."""
        ...


@runtime_checkable
class JournalSnapshotRepository(Protocol):
    """Checkpoints of one stream's state.

    The state is the domain's :class:`~plantkeeper.domain.journal.state.JournalState`,
    so the port carries no serialisation concern: that belongs to the storage.
    """

    async def latest(self, plant_id: PlantId) -> JournalState | None:
        """Return the newest checkpoint of the plant's journal, or ``None``."""
        ...

    async def save(self, state: JournalState) -> None:
        """Stage the checkpoint in the caller's transaction."""
        ...
