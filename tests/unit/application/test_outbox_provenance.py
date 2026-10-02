"""The provenance of an outbox row, for the three edges task 1.3 names.

Each edge binds a context and appends its events while that binding is current;
the row has to come out stamped with it. These tests drive the adapter and the
context managers directly, so the propagation is pinned without a broker, a
database or a process: ``tests/e2e/test_provenance.py`` runs the same three edges
for real, and ``tests/unit/infrastructure/test_decoding.py`` pins the step before
this one — a delivery's headers becoming the context bound here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from plantkeeper.api.main import API_RAISER
from plantkeeper.application.provenance import (
    UNKNOWN_RAISER,
    acting_as,
    async_provenance_scope,
    current_provenance,
    provenance_scope,
    reaction_context,
    request_context,
)
from plantkeeper.domain.care.events import WateringDue
from plantkeeper.domain.identifiers import PlantId
from plantkeeper.infrastructure.persistence.mappers.outbox import outbox_model_from_event

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

PLANT = PlantId.new()


def a_watering_due() -> WateringDue:
    """A representative event raised by each of the three edges."""
    return WateringDue(plant_id=PLANT, due_at=NOW, occurred_at=NOW)


# --- The API command ----------------------------------------------------------


def test_a_command_s_stamps_the_request_s_conversation() -> None:
    """The first edge: an HTTP request raises an event while its context is bound."""
    conversation = uuid4()
    context = request_context(raised_by=API_RAISER, correlation_id=conversation)

    with provenance_scope(context):
        row = outbox_model_from_event(a_watering_due())

    assert row.raised_by == API_RAISER
    assert row.correlation_id == conversation
    assert row.causation_id is None
    assert row.created_at == context.observed_at


# --- The consumer reaction ----------------------------------------------------


async def test_a_reaction_is_caused_by_the_delivery_it_handles() -> None:
    """The second edge: a consumer copies correlation and causation off the delivery."""
    conversation = uuid4()
    delivered = uuid4()
    context = reaction_context(
        raised_by=API_RAISER, caused_by=delivered, correlation_id=conversation
    )

    async with async_provenance_scope(context):
        row = outbox_model_from_event(a_watering_due())

    assert row.raised_by == API_RAISER
    assert row.causation_id == delivered
    assert row.correlation_id == conversation


async def test_a_reaction_to_an_unattributed_event_starts_a_conversation() -> None:
    """A delivery with no correlation of its own is still a link in a chain.

    The cause becomes the conversation rather than the chain losing its head,
    which is the tolerance policy ``docs/events.md`` promises for a message
    published before the headers existed.
    """
    delivered = uuid4()
    context = reaction_context(raised_by=UNKNOWN_RAISER, caused_by=delivered)

    async with async_provenance_scope(context):
        row = outbox_model_from_event(a_watering_due())

    assert row.raised_by == UNKNOWN_RAISER
    assert row.correlation_id == delivered
    assert row.causation_id == delivered


async def test_events_of_one_delivery_share_an_instant() -> None:
    """``observed_at`` is the edge's clock reading, not the row's insert time.

    Two rows appended by one delivery carry the same instant, so a chain sorts by
    when the work happened rather than by the order the rows were flushed in.
    """
    context = reaction_context(raised_by=API_RAISER, caused_by=uuid4())

    async with async_provenance_scope(context):
        first = outbox_model_from_event(a_watering_due())
        second = outbox_model_from_event(a_watering_due())

    assert first.created_at == second.created_at == context.observed_at


async def test_the_binding_does_not_leak_past_the_delivery() -> None:
    """One delivery's conversation must not become the next one's."""
    async with async_provenance_scope(reaction_context(raised_by=API_RAISER, caused_by=uuid4())):
        pass
    assert current_provenance() is None


# --- The saga step ------------------------------------------------------------


async def test_a_saga_step_is_raised_by_the_saga_and_keeps_the_conversation() -> None:
    """The third edge: the step is the saga's work, inside the delivery's conversation."""
    conversation = uuid4()
    delivered = uuid4()
    saga_id = uuid4()

    async with (
        async_provenance_scope(
            reaction_context(raised_by=API_RAISER, caused_by=delivered, correlation_id=conversation)
        ),
        acting_as(f"saga:{saga_id}"),
    ):
        row = outbox_model_from_event(a_watering_due())

    assert row.raised_by == f"saga:{saga_id}"
    assert row.correlation_id == conversation
    assert row.causation_id == delivered


async def test_a_saga_with_no_ambient_context_stands_alone() -> None:
    """A recovery job resuming a process has no delivery to point at."""
    async with acting_as("saga:recovery"):
        row = outbox_model_from_event(a_watering_due())

    assert row.raised_by == "saga:recovery"
    assert row.correlation_id is None
    assert row.causation_id is None


# --- The tolerance policy -----------------------------------------------------


def test_a_row_appended_with_no_context_names_an_unknown_origin() -> None:
    """Absent provenance is tolerated, never fatal: the row is still valid."""
    row = outbox_model_from_event(a_watering_due())

    assert row.raised_by == UNKNOWN_RAISER
    assert row.correlation_id is None
    assert row.causation_id is None
    assert row.traceparent is None


def test_the_row_carries_the_event_s_own_schema_version() -> None:
    """The version travels with the event, so a consumer can branch on it."""
    with provenance_scope(request_context(raised_by=API_RAISER)):
        row = outbox_model_from_event(a_watering_due())

    assert row.schema_version == WateringDue.schema_version
