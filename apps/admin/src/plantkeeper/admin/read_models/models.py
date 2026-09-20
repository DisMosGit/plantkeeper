"""The models Django Admin shows and the projections write.

One table per fact the read side needs, all in the ``read_analytics`` schema. They
are not the write side's tables under another name: ``plants`` carries a
denormalised ``species_name`` and the next watering moment, and every column is
owned by exactly one projection (see ``plantkeeper.admin.projections``).

``processed_events`` is the idempotency ledger every Kafka consumer has to keep —
``AGENTS.md`` requires idempotency on ``(consumer_group, event_id)`` — and a
projection writes it in the same transaction as the rows it projects.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from django.db import models
from pydantic import JsonValue

from plantkeeper.infrastructure.persistence.schemas import READ_ANALYTICS


def read_table(name: str) -> str:
    """Return the schema-qualified table name a model should use.

    ``Meta.db_table`` is handed to PostgreSQL's ``quote_name`` verbatim, and that
    function leaves an already-quoted name alone, so this is how a Django model is
    placed in a named schema without a database router.
    """
    return f'"{READ_ANALYTICS}"."{name}"'


class PlantReadModel(models.Model):
    """One plant, as the Garden and Care projections know it.

    A row is assembled from three event streams — garden, care and catalog — and
    the columns each one owns are marked below. The garden-owned columns are
    nullable on purpose: a care or journal event can be projected before the
    garden event that carries the plant's name (the topics are consumed
    independently), and a partial row is a better answer than a lost fact. The
    write side still guarantees every value is eventually filled in.
    """

    plant_id: UUID = models.UUIDField(primary_key=True)
    # --- Garden-owned ---------------------------------------------------------
    household_id: UUID | None = models.UUIDField(null=True, blank=True)
    species_id: UUID | None = models.UUIDField(null=True, blank=True)
    name: str | None = models.CharField(max_length=200, null=True, blank=True)
    location: str | None = models.CharField(max_length=100, null=True, blank=True)
    added_at: datetime | None = models.DateTimeField(null=True, blank=True)
    removed: bool = models.BooleanField(default=False)
    onboarded_at: datetime | None = models.DateTimeField(null=True, blank=True)
    # --- Catalog-owned --------------------------------------------------------
    species_name: str | None = models.CharField(max_length=200, null=True, blank=True)
    # --- Care-owned -----------------------------------------------------------
    next_watering_at: datetime | None = models.DateTimeField(null=True, blank=True)

    updated_at: datetime = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = read_table("plants")
        indexes = [
            models.Index(fields=["household_id", "removed"], name="ix_plants_household_id_removed")
        ]
        ordering = ["added_at"]

    def __str__(self) -> str:
        """Identify the row the way the admin lists it."""
        return self.name or f"plant {self.plant_id}"


class CareReadModel(models.Model):
    """One plant's watering schedule."""

    plant_id: UUID = models.UUIDField(primary_key=True)
    watering_interval: timedelta = models.DurationField()
    next_watering_at: datetime = models.DateTimeField()
    last_watered_at: datetime | None = models.DateTimeField(null=True, blank=True)
    version: int = models.IntegerField(default=1)
    updated_at: datetime = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = read_table("care_schedules")
        indexes = [models.Index(fields=["next_watering_at"], name="ix_care_next_watering_at")]
        ordering = ["next_watering_at"]

    def __str__(self) -> str:
        """Identify the row by the plant it belongs to."""
        return f"care for {self.plant_id}"


class SpeciesReadModel(models.Model):
    """One catalogue entry, kept next to the plants that reference it."""

    species_id: UUID = models.UUIDField(primary_key=True)
    scientific_name: str = models.CharField(max_length=200)
    common_name: str = models.CharField(max_length=200)
    watering_interval: timedelta = models.DurationField()
    light_requirement: str = models.CharField(max_length=20)
    version: int = models.IntegerField(default=1)
    updated_at: datetime = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = read_table("species")
        ordering = ["common_name"]
        verbose_name_plural = "species"

    def __str__(self) -> str:
        """Identify the row the way a plant list reads it."""
        return self.common_name


class NotificationReadModel(models.Model):
    """One notification the household should see."""

    notification_id: UUID = models.UUIDField(primary_key=True)
    household_id: UUID = models.UUIDField()
    notification_type: str = models.CharField(max_length=40)
    payload: dict[str, JsonValue] = models.JSONField(default=dict)
    created_at: datetime = models.DateTimeField()
    read_at: datetime | None = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = read_table("notifications")
        indexes = [
            models.Index(fields=["household_id", "read_at"], name="ix_notifications_house_read")
        ]
        ordering = ["-created_at"]

    def __str__(self) -> str:
        """Identify the row by its type and household."""
        return f"{self.notification_type} for {self.household_id}"


class JournalReadModel(models.Model):
    """One immutable care record.

    ``plant`` is a real foreign key rather than a loose identifier because the
    admin shows these inline on the plant page; the key is what makes that
    possible, and ``on_delete=CASCADE`` keeps the read side self-consistent.
    """

    entry_id: UUID = models.UUIDField(primary_key=True)
    plant: PlantReadModel = models.ForeignKey(
        PlantReadModel, on_delete=models.CASCADE, related_name="journal_entries"
    )
    entry_type: str = models.CharField(max_length=20)
    note: str | None = models.TextField(null=True, blank=True)
    occurred_at: datetime = models.DateTimeField()
    recorded_at: datetime = models.DateTimeField()

    class Meta:
        db_table = read_table("journal_entries")
        indexes = [
            models.Index(fields=["plant", "occurred_at"], name="ix_journal_plant_occurred_at")
        ]
        ordering = ["-occurred_at"]
        verbose_name_plural = "journal entries"

    def __str__(self) -> str:
        """Identify the row by its type and moment."""
        return f"{self.entry_type} at {self.occurred_at:%Y-%m-%d}"


class ProcessedEvent(models.Model):
    """The ledger that makes every projection idempotent.

    The winning insert is what earns the right to project an event; a second
    delivery of the same ``event_id`` from the same consumer group finds the row
    and does nothing. See ``plantkeeper.admin.projections.base``.
    """

    consumer_group: str = models.CharField(max_length=100)
    event_id: UUID = models.UUIDField()
    processed_at: datetime = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = read_table("processed_events")
        constraints = [
            models.UniqueConstraint(
                fields=["consumer_group", "event_id"],
                name="uq_processed_events_consumer_group_event_id",
            )
        ]
        indexes = [models.Index(fields=["event_id"], name="ix_processed_events_event_id")]

    def __str__(self) -> str:
        """Identify the ledger row."""
        return f"{self.consumer_group}:{self.event_id}"
