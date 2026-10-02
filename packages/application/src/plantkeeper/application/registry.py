"""The command/query registry.

Every request type is bound to exactly one handler class here, and nowhere else.
The mediator resolves the handler *class* from the dependency container, so the
registry says what handles what while the container decides what it is
constructed with — the two concerns stay apart.
"""

from __future__ import annotations

from cqrs.requests.map import RequestMap

from plantkeeper.application.commands.care import (
    CreateCareScheduleCommand,
    CreateCareScheduleHandler,
    DeleteCareScheduleCommand,
    DeleteCareScheduleHandler,
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
    CreateOnboardingNotificationCommand,
    CreateOnboardingNotificationHandler,
    DeleteNotificationCommand,
    DeleteNotificationHandler,
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
    _bind_commands(request_map)

    # Garden
    request_map.bind(GetPlantQuery, GetPlantQueryHandler)
    request_map.bind(ListPlantsQuery, ListPlantsQueryHandler)
    request_map.bind(GetHouseholdQuery, GetHouseholdQueryHandler)

    # Care
    request_map.bind(GetTodayCareQuery, GetTodayCareQueryHandler)

    # Catalog
    request_map.bind(ListSpeciesQuery, ListSpeciesQueryHandler)
    request_map.bind(GetSpeciesQuery, GetSpeciesQueryHandler)

    # Telemetry
    request_map.bind(ListSensorsQuery, ListSensorsQueryHandler)

    # Notifications
    request_map.bind(ListPendingNotificationsQuery, ListPendingNotificationsHandler)

    # Journal
    request_map.bind(GetJournalTimelineQuery, GetJournalTimelineHandler)
    request_map.bind(GetJournalAtDateQuery, GetJournalAtDateHandler)

    return request_map


def build_command_map() -> RequestMap:
    """Return the map from *command* type to handler type.

    The command dispatcher resolves recorded cross-context commands through this
    map rather than the full one, and the difference is not cosmetic: Dishka builds
    a handler's whole dependency graph as soon as it is resolved, so a worker that
    only executes commands would otherwise need the read-side engine its queries
    depend on. Narrowing the map keeps a process's dependencies equal to the work it
    actually does.
    """
    request_map = RequestMap()
    _bind_commands(request_map)
    return request_map


def _bind_commands(request_map: RequestMap) -> None:
    """Bind every command, in the order the registry lists them."""
    # Garden
    request_map.bind(CreateHouseholdCommand, CreateHouseholdHandler)
    request_map.bind(AddPlantCommand, AddPlantHandler)
    request_map.bind(RemovePlantCommand, RemovePlantHandler)
    request_map.bind(MovePlantCommand, MovePlantHandler)

    # Care
    request_map.bind(CreateCareScheduleCommand, CreateCareScheduleHandler)
    request_map.bind(DeleteCareScheduleCommand, DeleteCareScheduleHandler)
    request_map.bind(WaterPlantCommand, WaterPlantHandler)
    request_map.bind(SkipWateringCommand, SkipWateringHandler)

    # Catalog
    request_map.bind(RequestSpeciesSyncCommand, RequestSpeciesSyncHandler)

    # Telemetry
    request_map.bind(AddSensorCommand, AddSensorHandler)
    request_map.bind(RemoveSensorCommand, RemoveSensorHandler)

    # Notifications
    request_map.bind(AcknowledgeNotificationCommand, AcknowledgeNotificationHandler)
    request_map.bind(CreateOnboardingNotificationCommand, CreateOnboardingNotificationHandler)
    request_map.bind(DeleteNotificationCommand, DeleteNotificationHandler)

    # Journal
    request_map.bind(AddJournalEntryCommand, AddJournalEntryHandler)
