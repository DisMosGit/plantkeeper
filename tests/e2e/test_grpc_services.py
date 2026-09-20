"""End-to-end test of the gRPC surface.

The server is started in-process against the same containerised Postgres the rest
of the suite uses, and the client is the stub ``make proto`` generated — the path
``make grpc`` plus ``grpcurl`` would take, without needing grpcurl installed.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import grpc
import pytest
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from plantkeeper.api.grpc.generated.plantkeeper.v1 import (
    care_pb2,
    care_pb2_grpc,
    common_pb2,
    garden_pb2,
    garden_pb2_grpc,
)
from plantkeeper.domain.care.schedule import CareSchedule
from plantkeeper.domain.garden.household import Household
from plantkeeper.domain.garden.plant import Plant
from plantkeeper.domain.identifiers import SpeciesId
from plantkeeper.domain.values import Location, WateringInterval
from plantkeeper.infrastructure.persistence.uow import SqlAlchemyUnitOfWork

pytestmark = pytest.mark.slow

WEEK = timedelta(days=7)
"""The interval the seeded schedule uses."""

RPC_TIMEOUT = 10.0
"""Long enough for a real RPC, short enough to fail a hung one."""


def care_stub(channel: grpc.aio.Channel) -> care_pb2_grpc.CareServiceStub:
    """Build the Care stub.

    protoc does not generate a ``.pyi`` for ``_pb2_grpc`` modules, so the stub
    constructor is untyped; every request and response it carries is typed.
    """
    return care_pb2_grpc.CareServiceStub(channel)  # type: ignore[no-untyped-call]


def garden_stub(channel: grpc.aio.Channel) -> garden_pb2_grpc.GardenServiceStub:
    """Build the Garden stub, for the same reason as :func:`care_stub`."""
    return garden_pb2_grpc.GardenServiceStub(channel)  # type: ignore[no-untyped-call]


async def seed_plant_with_schedule(database: str) -> tuple[str, str]:
    """Insert a household with one plant whose watering is due now.

    A schedule due today is what ``GetTodayCare`` is for; onboarding (Phase 4)
    would create it in production, so the test writes it through the same unit of
    work the saga uses.
    """
    now = datetime.now(UTC)
    engine = create_async_engine(database)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            uow = SqlAlchemyUnitOfWork(session)
            async with uow:
                household = Household.create(name="Home")
                await uow.households.add(household)
                plant = Plant.add(
                    household_id=household.id,
                    species_id=SpeciesId.new(),
                    name="Fern",
                    location=Location(value="Shelf"),
                    now=now,
                )
                household.add_plant(plant.id)
                await uow.plants.add(plant)
                schedule = CareSchedule.create(
                    plant_id=plant.id,
                    watering_interval=WateringInterval(value=WEEK),
                    starts_at=now,
                    now=now,
                )
                await uow.care_schedules.add(schedule)
                await uow.commit()
                return str(household.id), str(plant.id)
    finally:
        await engine.dispose()


async def test_get_today_care_returns_a_due_schedule(
    database: str, grpc_channel: grpc.aio.Channel
) -> None:
    household_id, plant_id = await seed_plant_with_schedule(database)
    response = await care_stub(grpc_channel).GetTodayCare(
        care_pb2.GetTodayCareRequest(household_id=common_pb2.HouseholdId(value=household_id)),
        timeout=RPC_TIMEOUT,
    )
    assert [item.plant_id.value for item in response.items] == [plant_id]
    assert response.items[0].version == 1
    assert response.items[0].watering_interval.ToTimedelta() == WEEK


async def test_completing_a_watering_advances_the_schedule(
    database: str, grpc_channel: grpc.aio.Channel
) -> None:
    _, plant_id = await seed_plant_with_schedule(database)
    stub = care_stub(grpc_channel)
    watered = await stub.CompleteWatering(
        care_pb2.CompleteWateringRequest(plant_id=common_pb2.PlantId(value=plant_id)),
        timeout=RPC_TIMEOUT,
    )
    assert watered.schedule.plant_id.value == plant_id
    assert watered.schedule.version == 2

    # The Garden aggregate refuses a second watering within the hour: a broken
    # invariant is FAILED_PRECONDITION, never a 500-shaped INTERNAL.
    with pytest.raises(grpc.aio.AioRpcError) as caught:
        await stub.CompleteWatering(
            care_pb2.CompleteWateringRequest(plant_id=common_pb2.PlantId(value=plant_id)),
            timeout=RPC_TIMEOUT,
        )
    assert caught.value.code() is grpc.StatusCode.FAILED_PRECONDITION


async def test_a_malformed_identifier_is_invalid_argument(grpc_channel: grpc.aio.Channel) -> None:
    with pytest.raises(grpc.aio.AioRpcError) as caught:
        await care_stub(grpc_channel).GetTodayCare(
            care_pb2.GetTodayCareRequest(household_id=common_pb2.HouseholdId(value="not-a-uuid")),
            timeout=RPC_TIMEOUT,
        )
    assert caught.value.code() is grpc.StatusCode.INVALID_ARGUMENT


async def test_list_plants_returns_the_household(
    database: str, grpc_channel: grpc.aio.Channel
) -> None:
    household_id, plant_id = await seed_plant_with_schedule(database)
    response = await garden_stub(grpc_channel).ListPlants(
        garden_pb2.ListPlantsRequest(household_id=common_pb2.HouseholdId(value=household_id)),
        timeout=RPC_TIMEOUT,
    )
    assert [item.plant_id.value for item in response.items] == [plant_id]
    assert response.items[0].name == "Fern"
    assert not response.items[0].HasField("last_watered_at")


async def test_get_plant_answers_not_found_for_an_unknown_plant(
    grpc_channel: grpc.aio.Channel,
) -> None:
    with pytest.raises(grpc.aio.AioRpcError) as caught:
        await garden_stub(grpc_channel).GetPlant(
            garden_pb2.GetPlantRequest(plant_id=common_pb2.PlantId(value=str(uuid4()))),
            timeout=RPC_TIMEOUT,
        )
    assert caught.value.code() is grpc.StatusCode.NOT_FOUND


async def test_health_reports_serving(grpc_channel: grpc.aio.Channel) -> None:
    stub = health_pb2_grpc.HealthStub(grpc_channel)
    response = await stub.Check(health_pb2.HealthCheckRequest(service=""), timeout=RPC_TIMEOUT)
    assert response.status is health_pb2.HealthCheckResponse.SERVING


async def test_reflection_lists_the_services(grpc_channel: grpc.aio.Channel) -> None:
    """Reflection is what makes ``grpcurl list`` work without the proto files."""
    stub = reflection_pb2_grpc.ServerReflectionStub(grpc_channel)
    call = stub.ServerReflectionInfo()
    await call.write(reflection_pb2.ServerReflectionRequest(list_services=""))
    message = await call.read()
    names = {service.name for service in message.list_services_response.service}
    await call.done_writing()
    call.cancel()
    assert {"plantkeeper.v1.CareService", "plantkeeper.v1.GardenService"} <= names
