"""Response DTOs of the application layer.

A view is what a use case answers with. It is not an aggregate — it is the flat,
already-consistent projection of one — and it is not an HTTP schema either: the
API layer maps a view onto its own response model so that the wire format can
change without touching the use cases.

Views subclass cqrs's ``PydanticResponse`` so that a handler's return type is the
framework's own response type. Identifiers stay domain value objects here; the
JSON form is a plain UUID, which is what a client should see.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from cqrs.response import PydanticResponse
from pydantic import AwareDatetime, ConfigDict, JsonValue

from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.catalog.species import Species
from plantkeeper.domain.catalog.values import LightRequirement
from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import (
    HouseholdId,
    JournalEntryId,
    NotificationId,
    PlantId,
    SensorId,
    SpeciesId,
)
from plantkeeper.domain.journal.entry import JournalEntry
from plantkeeper.domain.journal.state import JournalEntryState, JournalState
from plantkeeper.domain.journal.values import JournalEntryType
from plantkeeper.domain.notifications.notification import Notification
from plantkeeper.domain.notifications.values import NotificationType
from plantkeeper.domain.telemetry.sensor import Sensor

_VIEW_CONFIG = ConfigDict(frozen=True, extra="forbid")


class CollectionView[ItemT: PydanticResponse](PydanticResponse):
    """A list answer.

    ``RequestHandler`` answers with one response object, and a bare ``list`` would
    leave no room to add a count or a cursor later without breaking every client,
    so a list is wrapped rather than returned raw.
    """

    model_config = _VIEW_CONFIG

    items: list[ItemT]


class SyncRequestedView(PydanticResponse):
    """The acknowledgement of a request whose real work happens elsewhere."""

    model_config = _VIEW_CONFIG

    requested: bool


class HouseholdView(PydanticResponse):
    """One household and how many plants it owns."""

    model_config = _VIEW_CONFIG

    household_id: HouseholdId
    name: str
    plant_count: int

    @classmethod
    def from_domain(cls, household: Household) -> HouseholdView:
        """Project the aggregate."""
        return cls(
            household_id=household.id,
            name=household.name,
            plant_count=household.plant_count,
        )


class PlantView(PydanticResponse):
    """One plant as the API reports it."""

    model_config = _VIEW_CONFIG

    plant_id: PlantId
    household_id: HouseholdId
    species_id: SpeciesId
    name: str
    location: str
    added_at: AwareDatetime
    last_watered_at: AwareDatetime | None
    removed: bool

    @classmethod
    def from_domain(cls, plant: Plant) -> PlantView:
        """Project the aggregate."""
        return cls(
            plant_id=plant.id,
            household_id=plant.household_id,
            species_id=plant.species_id,
            name=plant.name,
            location=plant.location.value,
            added_at=plant.added_at,
            last_watered_at=plant.last_watered_at,
            removed=plant.is_removed,
        )


class CareScheduleView(PydanticResponse):
    """One plant's watering schedule."""

    model_config = _VIEW_CONFIG

    plant_id: PlantId
    watering_interval: timedelta
    next_watering_at: AwareDatetime
    version: int

    @classmethod
    def from_domain(cls, schedule: CareSchedule) -> CareScheduleView:
        """Project the aggregate."""
        return cls(
            plant_id=schedule.plant_id,
            watering_interval=schedule.watering_interval.value,
            next_watering_at=schedule.next_watering_at,
            version=schedule.version,
        )


class SensorView(PydanticResponse):
    """One sensor and the plant it watches."""

    model_config = _VIEW_CONFIG

    sensor_id: SensorId
    plant_id: PlantId
    added_at: AwareDatetime
    last_seen_at: AwareDatetime | None

    @classmethod
    def from_domain(cls, sensor: Sensor) -> SensorView:
        """Project the aggregate."""
        return cls(
            sensor_id=sensor.id,
            plant_id=sensor.plant_id,
            added_at=sensor.added_at,
            last_seen_at=sensor.last_seen_at,
        )


class SpeciesView(PydanticResponse):
    """One catalogue entry."""

    model_config = _VIEW_CONFIG

    species_id: SpeciesId
    scientific_name: str
    common_name: str
    watering_interval: timedelta
    light_requirement: LightRequirement
    version: int

    @classmethod
    def from_domain(cls, species: Species) -> SpeciesView:
        """Project the aggregate."""
        return cls(
            species_id=species.id,
            scientific_name=species.scientific_name,
            common_name=species.common_name,
            watering_interval=species.watering_interval.value,
            light_requirement=species.light_requirement,
            version=species.version,
        )


class NotificationView(PydanticResponse):
    """One notification waiting for (or already shown to) a household."""

    model_config = _VIEW_CONFIG

    notification_id: NotificationId
    household_id: HouseholdId
    notification_type: NotificationType
    payload: dict[str, JsonValue]
    created_at: AwareDatetime
    read_at: AwareDatetime | None

    @classmethod
    def from_domain(cls, notification: Notification) -> NotificationView:
        """Project the aggregate."""
        return cls(
            notification_id=notification.id,
            household_id=notification.household_id,
            notification_type=notification.notification_type,
            payload=notification.payload,
            created_at=notification.created_at,
            read_at=notification.read_at,
        )


class JournalEntryView(PydanticResponse):
    """One care record, as the API reports it.

    ``occurred_at`` is when the care happened; when the entry was *recorded* is the
    event store's own concern and is deliberately not on the wire.
    """

    model_config = _VIEW_CONFIG

    entry_id: JournalEntryId
    plant_id: PlantId
    entry_type: JournalEntryType
    occurred_at: AwareDatetime
    note: str | None

    @classmethod
    def from_domain(cls, entry: JournalEntry) -> JournalEntryView:
        """Project the aggregate."""
        return cls(
            entry_id=entry.id,
            plant_id=entry.plant_id,
            entry_type=entry.entry_type,
            occurred_at=entry.occurred_at,
            note=entry.note,
        )

    @classmethod
    def from_state(cls, entry: JournalEntryState, *, plant_id: PlantId) -> JournalEntryView:
        """Project a checkpointed entry, whose plant is the stream's, not its own."""
        return cls(
            entry_id=entry.entry_id,
            plant_id=plant_id,
            entry_type=entry.entry_type,
            occurred_at=entry.occurred_at,
            note=entry.note,
        )


class JournalStateView(PydanticResponse):
    """The journal as it stood at one moment, plus the summary of what it holds."""

    model_config = _VIEW_CONFIG

    plant_id: PlantId
    as_of: AwareDatetime
    entry_count: int
    counts_by_type: dict[JournalEntryType, int]
    entries: list[JournalEntryView]

    @classmethod
    def from_state(cls, state: JournalState, *, as_of: datetime) -> JournalStateView:
        """Project a replayed state, stamped with the cut-off that produced it."""
        return cls(
            plant_id=state.plant_id,
            as_of=as_of,
            entry_count=state.entry_count,
            counts_by_type=state.counts_by_type(),
            entries=[
                JournalEntryView.from_state(entry, plant_id=state.plant_id)
                for entry in state.entries
            ],
        )
