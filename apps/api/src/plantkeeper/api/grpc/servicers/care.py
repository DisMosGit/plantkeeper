"""The ``CareService`` implementation.

Watering is the slow, human rhythm of the platform — what is due today, and
completing one of those waterings — so it is the first service the gRPC surface
exposes. Both RPCs go through the same handlers the REST router calls.
"""

from __future__ import annotations

from typing import override

import grpc

from plantkeeper.api.grpc.generated.plantkeeper.v1 import care_pb2, care_pb2_grpc
from plantkeeper.api.grpc.mapping import (
    care_schedule_to_proto,
    household_id_from_proto,
    plant_id_from_proto,
)
from plantkeeper.api.grpc.servicers.base import BaseServicer
from plantkeeper.api.mediator import view_of
from plantkeeper.application.commands.care import WaterPlantCommand
from plantkeeper.application.queries.care import GetTodayCareQuery
from plantkeeper.application.views import CareScheduleView, CollectionView


class CareServicer(BaseServicer, care_pb2_grpc.CareServiceServicer):
    """What is due today, and completing or rescheduling one watering."""

    @override
    async def GetTodayCare(
        self, request: care_pb2.GetTodayCareRequest, context: grpc.aio.ServicerContext
    ) -> care_pb2.GetTodayCareResponse:
        """Return the household's schedules that come due before midnight UTC."""
        async with self.rpc(context) as mediator:
            # Parsing is inside the scope on purpose: a malformed identifier is
            # the client's ``INVALID_ARGUMENT``, and `rpc` is what translates it.
            query = GetTodayCareQuery(household_id=household_id_from_proto(request.household_id))
            view = view_of(await mediator.send(query), CollectionView[CareScheduleView])
        return care_pb2.GetTodayCareResponse(
            items=[care_schedule_to_proto(item) for item in view.items]
        )

    @override
    async def CompleteWatering(
        self, request: care_pb2.CompleteWateringRequest, context: grpc.aio.ServicerContext
    ) -> care_pb2.CompleteWateringResponse:
        """Complete the watering and answer with the schedule for the next one."""
        async with self.rpc(context) as mediator:
            command = WaterPlantCommand(
                plant_id=plant_id_from_proto(request.plant_id),
                expected_version=(
                    request.expected_version if request.HasField("expected_version") else None
                ),
            )
            view = view_of(await mediator.send(command), CareScheduleView)
        return care_pb2.CompleteWateringResponse(schedule=care_schedule_to_proto(view))
