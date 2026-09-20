"""URLs of the read side.

The read side has no HTTP API of its own: the write API answers commands and
queries, and this process exists to project events and to show Django Admin.
Later phases add routes (the notification long poll) to the Starlette application
rather than here.
"""

from __future__ import annotations

from django.contrib import admin
from django.urls import URLPattern, path

urlpatterns: list[URLPattern] = [
    path("admin/", admin.site.urls),
]
