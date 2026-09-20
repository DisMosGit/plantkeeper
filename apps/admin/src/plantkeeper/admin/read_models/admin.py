"""Django Admin for the read side.

The admin is a *view* of the read models, not an editor of them: every model here
is written by a projection, so an edit would be overwritten by the next event and
a deletion would simply come back on the next replay. The write paths are
therefore switched off, and Django renders the changelists and read-only detail
pages instead.

``admin.py`` is inside the ``read_models`` app because that is what Django's app
loader discovers: an ``AdminConfig`` is not needed, and the registrations below
are the whole configuration.
"""

from __future__ import annotations

from django.contrib import admin
from django.http import HttpRequest

from plantkeeper.admin.read_models.models import (
    CareReadModel,
    JournalReadModel,
    NotificationReadModel,
    PlantReadModel,
    ProcessedEvent,
    SpeciesReadModel,
)

admin.site.site_header = "PlantKeeper — read side"
admin.site.site_title = "PlantKeeper"
admin.site.index_title = "Projections"


class ReadOnlyMixin:
    """Switch off the three write paths of an admin.

    Django keeps the changelist and the detail page, because the auto-selected
    local user is a superuser and therefore has view permission; what disappears
    is the ability to add, edit or delete a projected row.

    ``obj`` is part of every signature because an inline is asked about its parent
    object as well as the request, and the default lets one mixin serve both.
    """

    def has_add_permission(self, request: HttpRequest, obj: object | None = None) -> bool:
        """A row appears because an event was projected, not because someone typed it."""
        return False

    def has_change_permission(self, request: HttpRequest, obj: object | None = None) -> bool:
        """Projections own these rows."""
        return False

    def has_delete_permission(self, request: HttpRequest, obj: object | None = None) -> bool:
        """A projection would recreate the row on the next replay."""
        return False


class JournalInline(ReadOnlyMixin, admin.TabularInline):
    """The plant's journal, on the plant's page.

    ``show_change_link`` is on so a long entry can be read on its own page without
    leaving the plant.
    """

    model = JournalReadModel
    extra = 0
    can_delete = False
    show_change_link = True
    fields = ("occurred_at", "entry_type", "note", "recorded_at")
    readonly_fields = fields
    ordering = ("-occurred_at",)


@admin.register(PlantReadModel)
class PlantAdmin(ReadOnlyMixin, admin.ModelAdmin):
    """Every plant the read side has projected."""

    list_display = ("name", "species_name", "location", "next_watering_at", "removed")
    list_filter = ("removed", "location")
    search_fields = ("name", "species_name", "location")
    date_hierarchy = "added_at"
    inlines = (JournalInline,)
    fields = (
        "plant_id",
        "household_id",
        "species_id",
        "species_name",
        "name",
        "location",
        "added_at",
        "onboarded_at",
        "next_watering_at",
        "removed",
        "updated_at",
    )
    readonly_fields = fields


@admin.register(CareReadModel)
class CareAdmin(ReadOnlyMixin, admin.ModelAdmin):
    """The watering schedule of every plant the read side has projected."""

    list_display = (
        "plant_id",
        "watering_interval",
        "next_watering_at",
        "last_watered_at",
        "version",
    )
    date_hierarchy = "next_watering_at"
    fields = (
        "plant_id",
        "watering_interval",
        "next_watering_at",
        "last_watered_at",
        "version",
        "updated_at",
    )
    readonly_fields = fields


@admin.register(SpeciesReadModel)
class SpeciesAdmin(ReadOnlyMixin, admin.ModelAdmin):
    """The catalogue as the read side knows it.

    Empty until the Catalogue publishes ``SpeciesUpdated``; see
    ``docs/cqrs.md``.
    """

    list_display = (
        "common_name",
        "scientific_name",
        "light_requirement",
        "watering_interval",
        "version",
    )
    search_fields = ("scientific_name", "common_name")
    list_filter = ("light_requirement",)
    fields = (
        "species_id",
        "scientific_name",
        "common_name",
        "light_requirement",
        "watering_interval",
        "version",
        "updated_at",
    )
    readonly_fields = fields


@admin.register(NotificationReadModel)
class NotificationAdmin(ReadOnlyMixin, admin.ModelAdmin):
    """What the household has been told, and what it acknowledged."""

    list_display = ("notification_type", "household_id", "created_at", "read_at")
    list_filter = ("notification_type", "read_at")
    date_hierarchy = "created_at"
    fields = (
        "notification_id",
        "household_id",
        "notification_type",
        "payload",
        "created_at",
        "read_at",
    )
    readonly_fields = fields


@admin.register(JournalReadModel)
class JournalAdmin(ReadOnlyMixin, admin.ModelAdmin):
    """Every care record the read side has projected."""

    list_display = ("occurred_at", "plant", "entry_type", "recorded_at")
    list_filter = ("entry_type",)
    search_fields = ("note",)
    date_hierarchy = "occurred_at"
    fields = ("entry_id", "plant", "entry_type", "note", "occurred_at", "recorded_at")
    readonly_fields = fields


@admin.register(ProcessedEvent)
class ProcessedEventAdmin(ReadOnlyMixin, admin.ModelAdmin):
    """The idempotency ledger, which is also how a stuck projection is debugged.

    Filtering by ``consumer_group`` answers "what has this projection seen?" for
    the group that appears to be behind.
    """

    list_display = ("consumer_group", "event_id", "processed_at")
    list_filter = ("consumer_group",)
    date_hierarchy = "processed_at"
    fields = ("consumer_group", "event_id", "processed_at")
    readonly_fields = fields
