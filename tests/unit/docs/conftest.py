"""The diagrams are statements about the whole platform, so the tests need Django.

``docs/diagrams/event-flow.md`` includes the read side's projections: without them,
every event that only a read model consumes would be reported as consumed by
nobody. Rendering it therefore means importing ``plantkeeper.admin.projections``,
and that means Django configured first — the same bootstrap as
``tests/unit/contracts/conftest.py`` and ``tests/unit/admin/conftest.py``. Nothing
connects to a database during ``setup()``.
"""

from __future__ import annotations

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "plantkeeper.admin.settings")
os.environ.setdefault("DJANGO_DEBUG", "true")
django.setup()
