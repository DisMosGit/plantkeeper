"""Django has to be configured before a read-side projection can be imported.

The same bootstrap as ``tests/unit/admin/conftest.py``, repeated here because pytest
imports a conftest only for the directory being collected: this package tests the
generated *contracts*, and one of them is the read side's AsyncAPI document, which
cannot be built without Django's app registry. Nothing connects to a database
during ``setup()``.
"""

from __future__ import annotations

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "plantkeeper.admin.settings")
os.environ.setdefault("DJANGO_DEBUG", "true")
django.setup()
