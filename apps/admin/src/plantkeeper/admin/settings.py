"""Django settings of the read side.

The read side runs on the Postgres instance of its own: it owns the
``read_analytics`` and ``read_telemetry`` schemas, whose tables come from Django
migrations, and it reads the same ``.env`` the services read — through
:class:`~plantkeeper.infrastructure.config.Settings` — so credentials exist in one
place instead of two. Pointing the ``READ_POSTGRES_*`` values at the write instance
collapses the two back into one database; that is configuration, not code.

There is no application authentication. Django Admin, however, renders as a user
and needs its own ``django.contrib.auth`` tables; a single local superuser is
selected automatically by ``plantkeeper.admin.dev_auth``, and
``DJANGO_AUTO_LOGIN_USER`` (empty) turns that off and leaves the stock login form.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

from plantkeeper.infrastructure.config import Settings

BASE_DIR: Final = Path(__file__).resolve().parents[3]
"""``apps/admin``: where ``manage.py`` and the collected static files live."""

_infrastructure: Final = Settings()


def _env_bool(name: str, *, default: bool) -> bool:
    """Read a boolean environment variable, accepting the usual spellings."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_list(name: str, *, default: list[str]) -> list[str]:
    """Read a comma-separated environment variable into a list."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


DEBUG: Final = _env_bool("DJANGO_DEBUG", default=True)

SECRET_KEY: Final = os.environ.get(
    "DJANGO_SECRET_KEY",
    # A development-only key: the read side has no sessions to forge and no
    # authentication to bypass beyond the auto-selected local user.
    "dev-only-insecure-secret-key",
)

ALLOWED_HOSTS: Final = ["*"] if DEBUG else _env_list("DJANGO_ALLOWED_HOSTS", default=[])

AUTO_LOGIN_USER: Final = os.environ.get("DJANGO_AUTO_LOGIN_USER", "admin").strip() or None
"""The local superuser Django Admin renders as; ``None`` restores the login form."""

INSTALLED_APPS: Final = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "plantkeeper.admin.read_models",
    "plantkeeper.admin.read_telemetry",
]

MIDDLEWARE: Final = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    # After AuthenticationMiddleware, so it can see whether the session already
    # selected the local user.
    "plantkeeper.admin.dev_auth.DevAutoLoginMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF: Final = "plantkeeper.admin.urls"

TEMPLATES: Final = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

ASGI_APPLICATION: Final = "plantkeeper.admin.asgi.create_admin_application"

DATABASES: Final = {
    "default": {
        # Django's PostgreSQL backend speaks psycopg 3; the write side's asyncpg
        # driver is not an option here.
        "ENGINE": "django.db.backends.postgresql",
        # The read instance, not the write instance: `read_analytics` (and the
        # ledger that guards it) is this process's own database. The five values
        # have the same shape as the write side's, so pointing them at the write
        # instance is the documented rollback (`docs/runbooks/local-topology.md`).
        "NAME": _infrastructure.read_postgres_db,
        "USER": _infrastructure.read_postgres_user,
        "PASSWORD": _infrastructure.read_postgres_password,
        "HOST": _infrastructure.read_postgres_host,
        "PORT": str(_infrastructure.read_postgres_port),
        # The admin process is long-lived: a pooled connection is worth keeping
        # for a minute rather than reopening it for every page.
        "CONN_MAX_AGE": 60,
    }
}

# The auto-selected local user has an unusable password, and there is no form to
# validate — see the module docstring.
AUTH_PASSWORD_VALIDATORS: Final[list[dict[str, str]]] = []

LANGUAGE_CODE: Final = "en-us"
TIME_ZONE: Final = "UTC"
USE_I18N: Final = True
USE_TZ: Final = True

STATIC_URL: Final = "/static/"
STATIC_ROOT: Final = BASE_DIR / "staticfiles"

DEFAULT_AUTO_FIELD: Final = "django.db.models.BigAutoField"
