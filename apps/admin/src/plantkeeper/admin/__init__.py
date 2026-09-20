"""PlantKeeper Admin: ASGI Django read models and the Django Admin UI."""

from __future__ import annotations

import os

# Every entry point into this package — ``manage.py``, the ASGI factory, the test
# suite — needs Django to know which settings module it is configuring. Setting
# the default here means an import of ``plantkeeper.admin.settings`` or of a read
# model cannot fail with ``ImproperlyConfigured`` just because the caller forgot.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "plantkeeper.admin.settings")
