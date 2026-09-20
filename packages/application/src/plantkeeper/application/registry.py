"""The command/query registry.

Every request type is bound to exactly one handler class here, and nowhere else.
The mediator resolves the handler *class* from the dependency container, so the
registry says what handles what while the container decides what it is
constructed with — the two concerns stay apart.
"""

from __future__ import annotations

from cqrs.requests.map import RequestMap

from plantkeeper.application.commands.care import (
    SkipWateringCommand,
    SkipWateringHandler,
    WaterPlantCommand,
    WaterPlantHandler,
)
from plantkeeper.application.commands.catalog import (
    RequestSpeciesSyncCommand,
    RequestSpeciesSyncHandler,
)
from plantkeeper.application.commands.garden import (
    AddPlantCommand,
    AddPlantHandler,
    CreateHouseholdCommand,
    CreateHouseholdHandler,
    MovePlantCommand,
    MovePlantHandler,
    RemovePlantCommand,
    RemovePlantHandler,
)
from plantkeeper.application.commands.journal import (
    AddJournalEntryCommand,
    AddJournalEntryHandler,
)
from plantkeeper.application.commands.notifications import (
    AcknowledgeNotificationCommand,
    AcknowledgeNotificationHandler,
)
from plantkeeper.application.commands.telemetry import (
    AddSensorCommand,
    AddSensorHandler,
    RemoveSensorCommand,
    RemoveSensorHandler,
)
from plantkeeper.application.queries.care import GetTodayCareQuery, GetTodayCareQueryHandler
from plantkeeper.application.queries.catalog import (
    GetSpeciesQuery,
    GetSpeciesQueryHandler,
    ListSpeciesQuery,
    ListSpeciesQueryHandler,
)
from plantkeeper.application.queries.garden import (
    GetHouseholdQuery,
    GetHouseholdQueryHandler,
    GetPlantQuery,
    GetPlantQueryHandler,
    ListPlantsQuery,
    ListPlantsQueryHandler,
)
from plantkeeper.application.queries.journal import (
    GetJournalAtDateHandler,
    GetJournalAtDateQuery,
    GetJournalTimelineHandler,
    GetJournalTimelineQuery,
)
from plantkeeper.application.queries.notifications import (
    ListPendingNotificationsHandler,
    ListPendingNotificationsQuery,
)
from plantkeeper.application.queries.telemetry import ListSensorsQuery, ListSensorsQueryHandler


def build_request_map() -> RequestMap:
    """Return the map from request type to handler type for the whole write side."""
    request_map = RequestMap()

    # Garden
    request_map.bind(CreateHouseholdCommand, CreateHouseholdHandler)
    request_map.bind(AddPlantCommand, AddPlantHandler)
    request_map.bind(RemovePlantCommand, RemovePlantHandler)
    request_map.bind(MovePlantCommand, MovePlantHandler)
    request_map.bind(GetPlantQuery, GetPlantQueryHandler)
    request_map.bind(ListPlantsQuery, ListPlantsQueryHandler)
    request_map.bind(GetHouseholdQuery, GetHouseholdQueryHandler)

    # Care
    request_map.bind(WaterPlantCommand, WaterPlantHandler)
    request_map.bind(SkipWateringCommand, SkipWateringHandler)
    request_map.bind(GetTodayCareQuery, GetTodayCareQueryHandler)

    # Catalog
    request_map.bind(RequestSpeciesSyncCommand, RequestSpeciesSyncHandler)
    request_map.bind(ListSpeciesQuery, ListSpeciesQueryHandler)
    request_map.bind(GetSpeciesQuery, GetSpeciesQueryHandler)

    # Telemetry
    request_map.bind(AddSensorCommand, AddSensorHandler)
    request_map.bind(RemoveSensorCommand, RemoveSensorHandler)
    request_map.bind(ListSensorsQuery, ListSensorsQueryHandler)

    # Notifications
    request_map.bind(AcknowledgeNotificationCommand, AcknowledgeNotificationHandler)
    request_map.bind(ListPendingNotificationsQuery, ListPendingNotificationsHandler)

    # Journal
    request_map.bind(AddJournalEntryCommand, AddJournalEntryHandler)
    request_map.bind(GetJournalTimelineQuery, GetJournalTimelineHandler)
    request_map.bind(GetJournalAtDateQuery, GetJournalAtDateHandler)

    return request_map
