"""Saga-intent row mapping.

The row is the hand-off between a process manager and the context that will do
the work, so the mapper is deliberately thin: the payload is stored exactly as the
recording step built it, and read back exactly as it was stored. Anything the
mapper decided here would be a decision the dispatcher could not see.
"""

from __future__ import annotations

from plantkeeper.application.ports.saga_intents import IntentStatus, SagaIntent
from plantkeeper.infrastructure.persistence.models.shared import SagaIntentModel


def saga_intent_to_domain(model: SagaIntentModel) -> SagaIntent:
    """Turn a persisted intent row into the value the application layer reads."""
    return SagaIntent(
        id=model.id,
        saga_id=model.saga_id,
        step_no=model.step_no,
        command_name=model.command_name,
        payload=model.payload,
        status=IntentStatus(model.status),
        attempts=model.attempts,
        idempotency_key=model.idempotency_key,
        last_error=model.last_error,
    )
