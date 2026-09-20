"""The gRPC server process.

The server is a second wire protocol over the same application layer as the REST
API, not a second implementation: it resolves the same request map and the same
handlers through ``api_providers()``, and, like the REST process, it never builds
a Kafka broker (the outbox relay does the publishing). ``make grpc`` runs it.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager

import grpc
from cqrs.requests.map import RequestMap
from dishka import AsyncContainer, Provider, make_async_container
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from plantkeeper.api.grpc.generated.plantkeeper.v1 import (
    care_pb2,
    care_pb2_grpc,
    garden_pb2,
    garden_pb2_grpc,
)
from plantkeeper.api.grpc.servicers.care import CareServicer
from plantkeeper.api.grpc.servicers.garden import GardenServicer
from plantkeeper.infrastructure.di.providers import api_providers

logger = logging.getLogger(__name__)

DEFAULT_HOST = "0.0.0.0"
"""Where ``make grpc`` listens; the port is the documented ``:50051``."""

DEFAULT_PORT = 50051
"""The gRPC port, the way the REST API uses 8000 and the admin uses 8001."""

SHUTDOWN_GRACE_SECONDS = 5.0
"""How long ``server.stop`` waits for in-flight RPCs before aborting them."""


def service_names() -> tuple[str, ...]:
    """Return the services reflection advertises.

    Derived from the generated descriptors so a new service cannot be registered
    without appearing to grpcurl. The health service is named by hand because it
    comes from ``grpcio-health-checking``, not from ``proto/``.
    """
    return (
        care_pb2.DESCRIPTOR.services_by_name["CareService"].full_name,
        garden_pb2.DESCRIPTOR.services_by_name["GardenService"].full_name,
        health_pb2.DESCRIPTOR.services_by_name["Health"].full_name,
        reflection.SERVICE_NAME,
    )


async def build_grpc_server(
    container: AsyncContainer,
    request_map: RequestMap,
    *,
    host: str,
    port: int,
) -> tuple[grpc.aio.Server, int]:
    """Register the servicers on a started server and return it with its port.

    ``port=0`` asks the OS for a free port, which is what the tests use so they
    never collide with a running ``make grpc``.
    """
    server = grpc.aio.server()
    # The generated `_pb2_grpc` modules ship no `.pyi` stub, so their
    # registration functions are untyped; the servicers they register are fully
    # annotated, and `proto/` is the contract in either case.
    care_pb2_grpc.add_CareServiceServicer_to_server(  # type: ignore[no-untyped-call]
        CareServicer(container=container, request_map=request_map), server
    )
    garden_pb2_grpc.add_GardenServiceServicer_to_server(  # type: ignore[no-untyped-call]
        GardenServicer(container=container, request_map=request_map), server
    )
    health_servicer = health.aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    await health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    reflection.enable_server_reflection(service_names(), server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    await server.start()
    return server, bound_port


@asynccontextmanager
async def running_grpc_server(
    container: AsyncContainer, *, host: str = "127.0.0.1", port: int = 0
) -> AsyncIterator[tuple[grpc.aio.Server, int]]:
    """Run a server on a container the caller owns, stopping it on the way out."""
    request_map = await container.get(RequestMap)
    server, bound_port = await build_grpc_server(container, request_map, host=host, port=port)
    try:
        yield server, bound_port
    finally:
        await server.stop(SHUTDOWN_GRACE_SECONDS)


@asynccontextmanager
async def run_grpc_server(
    *, providers: Sequence[Provider] | None = None, host: str = "127.0.0.1", port: int = 0
) -> AsyncIterator[tuple[grpc.aio.Server, int]]:
    """Run a server on a container of its own, as the process and the tests do.

    ``providers`` exists for tests, the same way ``create_app`` takes them: a test
    points the settings and the clock at its own fixtures without touching the
    environment. The container is closed on the loop that built it, because the
    engine's pool is loop-bound.
    """
    container = make_async_container(*(providers if providers is not None else api_providers()))
    try:
        async with running_grpc_server(container, host=host, port=port) as handle:
            yield handle
    finally:
        await container.close()


async def _wait_for_shutdown() -> None:
    """Return when SIGINT or SIGTERM asks the process to stop.

    The handler flags the wait rather than cancelling anything: a stopped server
    should finish the RPC it is in, which is why this is an event and not a task
    cancellation.
    """
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for received in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(received, stop.set)
    await stop.wait()


async def serve() -> None:
    """Run the server at the documented host and port until a stop signal."""
    async with run_grpc_server(host=DEFAULT_HOST, port=DEFAULT_PORT) as (_, bound_port):
        logger.info("gRPC server started on %s:%s", DEFAULT_HOST, bound_port)
        await _wait_for_shutdown()
        logger.info("gRPC server stopping")


def configure_logging() -> None:
    """Configure the process's logging, as the worker does for itself."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )


def main() -> None:
    """Entry point for ``python -m plantkeeper.api.grpc``."""
    configure_logging()
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        # Only reached if the signal handler could not be installed (Windows).
        logger.info("interrupted")
