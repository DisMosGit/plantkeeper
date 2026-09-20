"""Django has to be configured before a projection can be imported.

Scoped to this directory on purpose: ``tests/unit/domain`` and
``tests/unit/infrastructure`` need no framework, and pytest imports a conftest
only for the directory being collected.
"""

from __future__ import annotations

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "plantkeeper.admin.settings")
os.environ.setdefault("DJANGO_DEBUG", "true")
django.setup()
