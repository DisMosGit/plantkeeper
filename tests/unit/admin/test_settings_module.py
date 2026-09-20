"""The read side's Django settings, which are configuration and need no database.

Importing the settings module does not configure Django — that happens on the
first access through ``django.conf`` — so these assertions are plain unit tests
that stay inside ``make test-unit``.
"""

from __future__ import annotations

from plantkeeper.admin import settings as django_settings
from plantkeeper.infrastructure.persistence.schemas import READ_ANALYTICS


def test_the_read_models_app_is_installed() -> None:
    """The projections' models are only discovered through an installed app."""
    assert "plantkeeper.admin.read_models" in django_settings.INSTALLED_APPS


def test_admin_is_installed_and_auto_logged_in() -> None:
    """The admin renders, and the middleware that selects the local user is in place."""
    assert "django.contrib.admin" in django_settings.INSTALLED_APPS
    assert "plantkeeper.admin.dev_auth.DevAutoLoginMiddleware" in django_settings.MIDDLEWARE
    assert django_settings.AUTO_LOGIN_USER


def test_the_database_is_the_write_side_postgres_over_psycopg() -> None:
    """One database, one set of credentials, and Django's own driver for it."""
    database = django_settings.DATABASES["default"]
    assert database["ENGINE"] == "django.db.backends.postgresql"
    assert database["NAME"]
    assert database["USER"]


def test_the_read_schema_is_not_the_write_schema() -> None:
    """The read side is a schema of its own: no migration may touch ``write_*``."""
    assert READ_ANALYTICS == "read_analytics"
    assert not READ_ANALYTICS.startswith("write_")


def test_time_is_timezone_aware() -> None:
    """Events carry aware datetimes; a naive column would silently drop the zone."""
    assert django_settings.USE_TZ is True
    assert django_settings.TIME_ZONE == "UTC"


def test_static_files_are_collected_where_make_admin_serves_them() -> None:
    """``make admin`` collects into ``STATIC_ROOT``; the Starlette mount reads it."""
    assert django_settings.STATIC_URL == "/static/"
    assert django_settings.STATIC_ROOT.name == "staticfiles"
    assert django_settings.STATIC_ROOT.parent.name == "admin"
