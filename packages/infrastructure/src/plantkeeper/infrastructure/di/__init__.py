"""Dependency injection: the Dishka providers and the cqrs container adapter."""

from __future__ import annotations

from plantkeeper.infrastructure.di.cqrs import DishkaCQRSContainer
from plantkeeper.infrastructure.di.providers import (
    AppProvider,
    DatabaseProvider,
    MessagingProvider,
    RepositoryProvider,
    SagaProvider,
    build_handler_provider,
    build_saga_component_provider,
    worker_providers,
)

__all__ = (
    "AppProvider",
    "DatabaseProvider",
    "DishkaCQRSContainer",
    "MessagingProvider",
    "RepositoryProvider",
    "SagaProvider",
    "build_handler_provider",
    "build_saga_component_provider",
    "worker_providers",
)
