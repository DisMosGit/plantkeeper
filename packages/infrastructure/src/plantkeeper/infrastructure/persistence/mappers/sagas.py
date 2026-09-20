"""Saga state and missed-care window <-> row mapping.

Unlike the aggregate mappers, these rows do not rebuild a domain aggregate: a
saga execution and a grace window are application records. The mapping still lives
in one module so a column rename touches one place.
"""

from __future__ import annotations

from cqrs.saga.storage.enums import SagaStatus

from plantkeeper.application.ports.sagas import MissedCareState, MissedCareWindow, SagaState
from plantkeeper.domain.identifiers import HouseholdId, PlantId
from plantkeeper.infrastructure.persistence.models.care import MissedCareWindowModel
from plantkeeper.infrastructure.persistence.models.shared import SagaStateModel


def saga_state_to_domain(model: SagaStateModel) -> SagaState:
    """Rebuild the read model of a saga execution from its row."""
    return SagaState(
        saga_id=model.id,
        saga_name=model.name,
        status=SagaStatus(model.status),
        version=model.version,
        recovery_attempts=model.recovery_attempts,
        created_at=model.created_at,
        updated_at=model.updated_at,
    )


def missed_care_window_to_domain(model: MissedCareWindowModel) -> MissedCareWindow:
    """Rebuild the grace window from its row."""
    return MissedCareWindow(
        plant_id=PlantId(model.plant_id),
        household_id=HouseholdId(model.household_id),
        due_at=model.due_at,
        grace_deadline=model.grace_deadline,
        state=MissedCareState(model.state),
        updated_at=model.updated_at,
    )


def missed_care_window_to_model(window: MissedCareWindow) -> MissedCareWindowModel:
    """Build the row that represents ``window``."""
    return MissedCareWindowModel(
        plant_id=window.plant_id.value,
        household_id=window.household_id.value,
        due_at=window.due_at,
        grace_deadline=window.grace_deadline,
        state=window.state.value,
        updated_at=window.updated_at,
    )
