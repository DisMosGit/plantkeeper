"""``manage.py`` for the read side.

Two jobs: apply the Django migrations that own ``read_analytics``, and collect the
Django Admin's static files so the Starlette mount in ``asgi.py`` can serve them.

The database URL is not passed on the command line: ``settings.py`` reads the same
``.env`` the other services read, exactly as ``alembic/env.py`` does for the write
schema.
"""

from __future__ import annotations

import os
import sys

from django.core.management import execute_from_command_line


def main() -> None:
    """Run the Django management command named in ``sys.argv``."""
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "plantkeeper.admin.settings")
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
