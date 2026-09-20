"""The ``GardenService`` implementation.

Reading the plants of a household; writes still belong to the REST API, which is
where idempotency keys and request bodies are expressed.
"""

from __future__ import annotations

from typing import override

import grpc

from plantkeeper.api.grpc.generated.plantkeeper.v1 import garden_pb2, garden_pb2_grpc
from plantkeeper.api.grpc.mapping import (
    household_id_from_proto,
    plant_id_from_proto,
    plant_to_proto,
)
from plantkeeper.api.grpc.servicers.base import BaseServicer
from plantkeeper.api.mediator import view_of
from plantkeeper.application.queries.garden import GetPlantQuery, ListPlantsQuery
from plantkeeper.application.views import CollectionView, PlantView


class GardenServicer(BaseServicer, garden_pb2_grpc.GardenServiceServicer):
    """The household's plants."""

    @override
    async def ListPlants(
        self, request: garden_pb2.ListPlantsRequest, context: grpc.aio.ServicerContext
    ) -> garden_pb2.ListPlantsResponse:
        """Return the household's plants, oldest first."""
        async with self.rpc(context) as mediator:
            query = ListPlantsQuery(
                household_id=household_id_from_proto(request.household_id),
                include_removed=request.include_removed,
            )
            view = view_of(await mediator.send(query), CollectionView[PlantView])
        return garden_pb2.ListPlantsResponse(items=[plant_to_proto(item) for item in view.items])

    @override
    async def GetPlant(
        self, request: garden_pb2.GetPlantRequest, context: grpc.aio.ServicerContext
    ) -> garden_pb2.GetPlantResponse:
        """Return one plant, or answer ``NOT_FOUND``."""
        async with self.rpc(context) as mediator:
            # Inside the scope: a malformed identifier is INVALID_ARGUMENT.
            query = GetPlantQuery(plant_id=plant_id_from_proto(request.plant_id))
            view = view_of(await mediator.send(query), PlantView)
        return garden_pb2.GetPlantResponse(plant=plant_to_proto(view))
