"""The event-sourced Journal aggregate.

``JournalEntry`` is one immutable fact; this aggregate is the whole journal of one
plant, rebuilt by replaying that plant's stream. The event store's stream id is
therefore the plant id, and a stream holds nothing but ``JournalEntryAdded``.

Three things live here and nowhere else:

* **replay** — :meth:`JournalAggregate.apply` folds one stored event into the
  state, and :meth:`from_state`/`state` checkpoint it. Applying an event records
  nothing: replay is not a new fact.
* **append** — :meth:`add_entry` is the only way a fact enters the stream. It
  delegates to ``JournalEntry.add`` so the event payload has exactly one producer,
  then drains that event onto the aggregate, which is what lets the caller hand it
  both to the event store and to the outbox.
* **the snapshot policy** — :meth:`should_snapshot` is a pure function of the
  version, so the interval stays a caller's decision (Phase 6 keeps it a module
  constant) and is testable without a database.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from plantkeeper.domain.base import AggregateRoot
from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.domain.journal.events import JournalEntryAdded
from plantkeeper.domain.journal.state import JournalEntryState, JournalState
from plantkeeper.domain.journal.stream import JournalStream
from plantkeeper.domain.journal.values import JournalEntryType


class JournalAggregate(AggregateRoot[PlantId]):
    """One plant's journal, as a consistency boundary over its event stream.

    The aggregate's own events are the events that have not reached the store yet:
    an aggregate freshly replayed from the store is clean, so committing a
    transaction can never re-publish history.
    """

    def __init__(
        self,
        plant_id: PlantId,
        *,
        entries: Sequence[JournalEntry] = (),
        version: int = 0,
    ) -> None:
        """Build the journal of ``plant_id``, optionally from a checkpoint."""
        super().__init__(plant_id)
        self._stream = JournalStream(plant_id, entries)
        self._version = version

    @property
    def version(self) -> int:
        """The stream version this aggregate has been replayed to."""
        return self._version

    @property
    def entries(self) -> tuple[JournalEntry, ...]:
        """The entries in chronological order."""
        return self._stream.entries

    def entry(self, entry_id: JournalEntryId) -> JournalEntry | None:
        """Return one replayed entry by identifier, or ``None``.

        The identity of a journal entry can be owned by the caller (a consumer
        deriving it from the event it reacts to), and this is what lets such a caller
        ask "is this fact already in the stream?" before trying to append it.
        """
        return self._stream.find(entry_id)

    @classmethod
    def from_state(cls, state: JournalState) -> JournalAggregate:
        """Rebuild the aggregate from a snapshot state."""
        return cls(
            state.plant_id,
            entries=[entry.to_domain(state.plant_id) for entry in state.entries],
            version=state.version,
        )

    def apply(self, event: JournalEntryAdded) -> None:
        """Fold one stored event into the state, recording nothing.

        A foreign plant or a duplicate entry is rejected by the stream itself, so a
        stream that does not belong to this aggregate fails loudly during replay
        rather than producing a plausible-looking journal.
        """
        self._stream.append(
            JournalEntry(
                event.entry_id,
                plant_id=event.plant_id,
                entry_type=event.entry_type,
                occurred_at=event.entry_occurred_at,
                note=event.note,
            )
        )
        self._version += 1

    def add_entry(
        self,
        *,
        entry_type: JournalEntryType,
        occurred_at: datetime,
        now: datetime,
        note: str | None = None,
        entry_id: JournalEntryId | None = None,
    ) -> JournalEntry:
        """Append one entry, recording a single ``JournalEntryAdded``.

        The event is drained off the entry and recorded on the aggregate, so
        ``collect_events()`` returns exactly the facts that still have to be stored
        and published.
        """
        entry = JournalEntry.add(
            plant_id=self.id,
            entry_type=entry_type,
            occurred_at=occurred_at,
            now=now,
            note=note,
            entry_id=entry_id,
        )
        for event in entry.collect_events():
            self._record(event)
        self._stream.append(entry)
        self._version += 1
        return entry

    def state(self) -> JournalState:
        """Checkpoint the whole journal at the current version."""
        return JournalState(
            plant_id=self.id,
            version=self._version,
            entries=tuple(JournalEntryState.from_domain(entry) for entry in self.entries),
        )

    def state_at(self, moment: datetime) -> JournalState:
        """The journal for care that happened at or before ``moment``.

        The cut-off is the *care* moment (``entry.occurred_at``), which is how the
        journal orders itself and what the admin timeline shows; the moment an entry
        was recorded is per-entry data rather than the query's axis. The comparison
        is inclusive and the version is the stream's, not the filtered count.
        """
        return JournalState(
            plant_id=self.id,
            version=self._version,
            entries=tuple(
                JournalEntryState.from_domain(entry)
                for entry in self.entries
                if entry.occurred_at <= moment
            ),
        )

    def should_snapshot(self, every: int) -> bool:
        """Whether the version is due a checkpoint, given an interval of ``every``."""
        return every > 0 and self._version > 0 and self._version % every == 0
