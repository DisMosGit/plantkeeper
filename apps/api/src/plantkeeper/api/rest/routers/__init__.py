"""The write API's routers, mounted under ``/api/v1``."""

from __future__ import annotations

from fastapi import APIRouter

from plantkeeper.api.rest.routers import (
    care,
    catalog,
    households,
    journal,
    notifications,
    plants,
    sensors,
)


def build_api_router() -> APIRouter:
    """Return one router containing every endpoint of the write API."""
    router = APIRouter()
    router.include_router(households.router)
    router.include_router(plants.router)
    router.include_router(care.router)
    router.include_router(sensors.router)
    router.include_router(catalog.router)
    router.include_router(journal.router)
    router.include_router(notifications.router)
    return router
