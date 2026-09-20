"""The Django app that owns the read models."""

from __future__ import annotations

from django.apps import AppConfig


class ReadModelsConfig(AppConfig):
    """Configuration for the projections' tables.

    ``label`` is deliberately short: it appears in admin URLs and in migration
    dependencies. The schema the tables live in is ``read_analytics`` and is set
    per model, not per app.
    """

    name = "plantkeeper.admin.read_models"
    label = "read_models"
    default_auto_field = "django.db.models.BigAutoField"
    verbose_name = "Read models"
