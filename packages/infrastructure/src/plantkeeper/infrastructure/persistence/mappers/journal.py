"""Journal aggregate <-> row mapping, including the event store's own encoding.

A stored event is the same document the outbox carries — ``type(event).__name__``
plus ``event.model_dump(mode="json")`` — so the store and the topic cannot drift
into two spellings of one fact. Decoding is the strict twin of the Kafka decoder:
an unknown type or a payload that no longer validates raises, because a local stream
has to be replayable in full and skipping a row would silently rewrite history.
"""

from __future__ import annotations

from pydantic import ValidationError

from plantkeeper.application.errors import EventStoreCorruptionError
from plantkeeper.application.ports.event_store import StoredEvent
from plantkeeper.domain.base import DomainEvent
from plantkeeper.domain.identifiers import JournalEntryId, PlantId
from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.domain.journal.state import JournalState
from plantkeeper.domain.journal.values import JournalEntryType
from plantkeeper.infrastructure.messaging.topics import event_type_for
from plantkeeper.infrastructure.persistence.models.journal import (
    EventStoreModel,
    JournalEntryModel,
    JournalSnapshotModel,
)


def journal_entry_to_domain(model: JournalEntryModel) -> JournalEntry:
    """Rebuild the ``JournalEntry`` aggregate from its row."""
    return JournalEntry(
        JournalEntryId(model.id),
        plant_id=PlantId(model.plant_id),
        entry_type=JournalEntryType(model.entry_type),
        occurred_at=model.occurred_at,
        note=model.note,
    )


def journal_entry_to_model(entry: JournalEntry) -> JournalEntryModel:
    """Build the row that represents ``entry``."""
    return JournalEntryModel(
        id=entry.id.value,
        plant_id=entry.plant_id.value,
        entry_type=entry.entry_type.value,
        occurred_at=entry.occurred_at,
        note=entry.note,
    )


def event_store_model_from_event(
    event: DomainEvent, *, stream_id: PlantId, version: int
) -> EventStoreModel:
    """Build the event-store row for ``event`` at ``version`` of ``stream_id``."""
    return EventStoreModel(
        stream_id=stream_id.value,
        version=version,
        event_id=event.event_id,
        event_type=type(event).__name__,
        payload=event.model_dump(mode="json"),
        occurred_at=event.occurred_at,
    )


def stored_event_from_model(model: EventStoreModel) -> StoredEvent:
    """Rebuild the domain event a stored row holds, or fail loudly."""
    event_type = event_type_for(model.event_type)
    if event_type is None:
        raise EventStoreCorruptionError(
            f"event store row {model.global_position} names unknown event {model.event_type!r}"
        )
    try:
        event = event_type.model_validate(model.payload)
    except ValidationError as exc:
        raise EventStoreCorruptionError(
            f"event store row {model.global_position} ({model.event_type}) failed validation"
        ) from exc
    return StoredEvent(
        stream_id=PlantId(model.stream_id),
        version=model.version,
        global_position=model.global_position,
        event=event,
    )


def snapshot_model_from_state(state: JournalState) -> JournalSnapshotModel:
    """Build the checkpoint row for ``state``."""
    return JournalSnapshotModel(
        stream_id=state.plant_id.value,
        version=state.version,
        state=state.model_dump(mode="json"),
    )


def state_from_snapshot_model(model: JournalSnapshotModel) -> JournalState:
    """Rebuild the checkpointed state, or fail loudly."""
    try:
        return JournalState.model_validate(model.state)
    except ValidationError as exc:
        raise EventStoreCorruptionError(
            f"journal snapshot for stream {model.stream_id} at version {model.version} "
            "failed validation"
        ) from exc
