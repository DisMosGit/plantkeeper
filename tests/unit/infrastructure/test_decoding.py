"""The consumer's decode policy, without a broker or a database.

``decode_event`` is where a Kafka delivery becomes a domain event, and where the
decision to *skip* rather than *fail* lives. These tests pin that policy: an
unknown event name, a missing header or a body that does not validate produces
``None`` — the consumer logs it and acknowledges — because one bad producer must
not be able to block a partition. A message that cannot be read at all is the only
case that should ever raise.

``decode_message`` wraps that with the delivery's provenance, and its policy is the
opposite one on purpose: provenance is decorative, so a missing or malformed
header is repaired rather than punished. The tests below pin both halves.

The policy is shared: the read side's projections and the write side's sagas both
call this function, so it is tested next to its implementation rather than inside
the app that used to own it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid7

from faststream.kafka import KafkaMessage
from faststream.kafka.message import ConsumerProtocol

from plantkeeper.application.provenance import UNKNOWN_RAISER
from plantkeeper.domain.garden.events import PlantAdded
from plantkeeper.domain.identifiers import HouseholdId, PlantId, SpeciesId
from plantkeeper.domain.values import Location
from plantkeeper.infrastructure.messaging.decoding import decode_event, decode_message


def plant_added() -> PlantAdded:
    """A valid Garden event, whose JSON form is a valid message body."""
    return PlantAdded(
        plant_id=PlantId.new(),
        household_id=HouseholdId.new(),
        species_id=SpeciesId.new(),
        name="Fern",
        location=Location(value="Shelf"),
        added_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    )


def message(
    body: Any,
    *,
    event_name: str | bytes | None = "PlantAdded",
    event_id: UUID | None = None,
    extra_headers: dict[str, Any] | None = None,
) -> KafkaMessage:
    """Build the message FastStream would hand the subscriber.

    The raw record is never read — the body and the headers are the whole
    contract — so a placeholder stands in for it and the consumer is left out.
    """
    headers: dict[str, Any] = {}
    if event_name is not None:
        headers["event_name"] = event_name
    if event_id is not None:
        headers["event_id"] = str(event_id)
    headers.update(extra_headers or {})
    # `decode_event` reads only the body and the headers, so the raw record and the
    # consumer are placeholders: building a real consumer would need a broker.
    return KafkaMessage(
        None,
        body,
        headers=headers,
        consumer=cast("ConsumerProtocol", None),
    )


def test_a_delivery_becomes_the_event_its_header_names() -> None:
    event = plant_added()

    decoded = decode_event(message(event.model_dump(mode="json"), event_id=event.event_id))

    assert isinstance(decoded, PlantAdded)
    assert decoded == event


def test_a_raw_byte_body_is_decoded() -> None:
    """FastStream's JSON parser usually decodes, but a custom one may not."""
    event = plant_added()

    decoded = decode_event(message(event.model_dump_json().encode("utf-8")))

    assert isinstance(decoded, PlantAdded)
    assert decoded.plant_id == event.plant_id


def test_a_header_that_is_not_text_is_read_as_text() -> None:
    """Kafka headers arrive as bytes; the ``event_name`` is what selects the model."""
    event = plant_added()

    decoded = decode_event(message(event.model_dump(mode="json"), event_name=b"PlantAdded"))

    assert isinstance(decoded, PlantAdded)


def test_a_message_without_an_event_name_is_skipped() -> None:
    assert decode_event(message(plant_added().model_dump(mode="json"), event_name=None)) is None


def test_an_unknown_event_name_is_skipped() -> None:
    """A newer producer, or a typo, must not stop the partition."""
    assert decode_event(message({"anything": True}, event_name="Invented")) is None


def test_a_body_that_does_not_validate_is_skipped() -> None:
    """A contract violation is logged with its ``event_id`` and dropped."""
    partial = {"name": "Fern"}

    assert decode_event(message(partial)) is None


def test_a_body_that_is_not_json_is_skipped_rather_than_raised() -> None:
    """A message body that cannot be parsed is a contract violation like any other.

    Raising here would take the delivery out of the failure policy, and a
    subscriber that redelivers a failed delivery would offer this one forever.
    """
    assert decode_event(message(b"not json at all", event_name="PlantAdded")) is None
    assert decode_event(message('["a list, not an event"]')) is None


# ---------------------------------------------------------------------------
# Provenance: repaired, never punished
# ---------------------------------------------------------------------------


def test_a_delivery_arrives_with_its_provenance() -> None:
    event = plant_added()
    correlation_id = uuid7()
    earlier = uuid7()
    observed_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    delivered = decode_message(
        message(
            event.model_dump(mode="json"),
            event_id=event.event_id,
            extra_headers={
                "correlation_id": str(correlation_id),
                "causation_id": str(earlier),
                "raised_by": "user:someone",
                "traceparent": "00-trace-span-01",
                "occurred_at": observed_at.isoformat(),
            },
        )
    )

    assert delivered is not None
    assert delivered.event == event
    assert delivered.context.correlation_id == correlation_id
    assert delivered.context.raised_by == "user:someone"
    assert delivered.context.traceparent == "00-trace-span-01"
    assert delivered.context.observed_at == observed_at
    # The context's cause is the *delivered* event, not the one that caused the
    # delivery: this context is what stamps the events the handler raises, and
    # their cause is what it is handling.
    assert delivered.context.causation_id == event.event_id


def test_the_delivered_event_becomes_the_cause_of_what_it_raises() -> None:
    """Whatever the delivery's own cause was, the *next* message points at this one."""
    event = plant_added()
    earlier = uuid7()

    delivered = decode_message(
        message(
            event.model_dump(mode="json"),
            event_id=event.event_id,
            extra_headers={"causation_id": str(earlier), "correlation_id": str(uuid7())},
        )
    )

    assert delivered is not None
    assert delivered.context.causation_id == event.event_id
    assert delivered.context.causation_id != earlier


def test_a_delivery_without_provenance_is_attributed_to_an_unknown_origin() -> None:
    """A message published before the headers existed is still handled."""
    event = plant_added()

    delivered = decode_message(message(event.model_dump(mode="json"), event_id=event.event_id))

    assert delivered is not None
    assert delivered.context.raised_by == UNKNOWN_RAISER
    # Its own identifier starts the conversation, so a chain from here is still
    # walkable rather than being lost.
    assert delivered.context.correlation_id == event.event_id
    assert delivered.context.causation_id == event.event_id
    assert delivered.context.traceparent is None


def test_a_malformed_provenance_header_does_not_cost_the_delivery() -> None:
    """A corrupted identifier is metadata damage, not a reason to drop the event."""
    event = plant_added()

    delivered = decode_message(
        message(
            event.model_dump(mode="json"),
            event_id=event.event_id,
            extra_headers={"correlation_id": "not-a-uuid", "occurred_at": "not-a-timestamp"},
        )
    )

    assert delivered is not None
    assert delivered.context.correlation_id == event.event_id
    assert delivered.context.observed_at is not None


def test_an_undecodable_message_yields_no_provenance_either() -> None:
    assert decode_message(message({"anything": True}, event_name="Invented")) is None
