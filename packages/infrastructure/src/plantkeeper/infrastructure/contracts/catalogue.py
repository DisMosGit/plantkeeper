"""The integration contract, derived from the code that implements it.

``docs/asyncapi-write.json`` and ``docs/asyncapi-read.json`` are generated, and
their consumer channels come from the FastStream routes the two processes
register. That leaves a hole in the middle: the *producers* on the write side are
not FastStream routes at all — the outbox relay publishes rows from
``write_shared.outbox`` — so a generated AsyncAPI document on its own cannot say
which event travels on which topic, or who consumes it.

This module closes that hole from the other direction. Every fact here is read out
of the registries that already exist: :data:`EVENT_TOPICS` maps each of the
catalogue's events to its topic, ``CONSUMER_TYPES``/``TRIGGER_TYPES`` say which
consumer handles which event, and ``SAGA_TYPES`` supplies the orchestration
sagas' step lists. Nothing is restated by hand, so a new event or consumer changes
the generated documents and diagrams by itself.

The result is:

* :func:`catalogue_rows` — the ``x-plantkeeper-event-catalogue`` payload, one row
  per event with its topic, producer and consumers, embedded in both AsyncAPI
  documents because a consumer's own channel listing cannot see the producers;
* :func:`event_flow_diagram` — the ``docs/diagrams/event-flow.md`` body, an edge
  set derived from the same rows;
* :func:`saga_diagrams` — the ``docs/diagrams/sagas.md`` body, rendered by
  ``python-cqrs``' own ``SagaMermaid`` so the diagram shows the real step classes
  and not a description of them.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import ClassVar, Final, Protocol, TypeVar

from cqrs.saga.mermaid import SagaMermaid

from plantkeeper.application.sagas.consumer import Consumer
from plantkeeper.application.sagas.registry import (
    CONSUMER_TYPES,
    SAGA_TYPES,
    TRIGGER_TYPES,
    saga_type_named,
)
from plantkeeper.domain.base import DomainEvent
from plantkeeper.infrastructure.messaging.topics import EVENT_TOPICS, EVENT_TYPES

EVENT_CONTEXTS: Final[dict[str, str]] = {
    "garden": "Garden",
    "care": "Care",
    "catalog": "Catalog",
    "journal": "Journal",
    "telemetry": "Telemetry",
    "notifications": "Notifications",
    "saga": "Saga",
}
"""The bounded context that owns each event, keyed by the module it is defined in.

Read out of the event class (``event.__module__``) rather than declared on it: a
hand-kept table of 26 entries would be a second catalogue to drift from.
"""

PRODUCER_OUTBOX_RELAY: Final = "Outbox relay"
"""The write side's producer: ``SqlAlchemyUnitOfWork.commit`` appends to the outbox
and ``OutboxRelay`` publishes what it finds."""

PRODUCER_TELEMETRY_INGRESS: Final = "Telemetry ingress"
"""The one producer that is not a command: the ingress stores a reading and
appends the ``TelemetryReceived`` it produces to the outbox in one transaction."""

PRODUCER_SAGA_ENGINE: Final = "Saga engine"
"""Where the four lifecycle events come from; they carry the saga's own name."""

CONSUMER_CONTEXTS: Final[dict[str, str]] = {
    "adaptive-watering": "Care",
    "missed-care": "Care",
    "journal-entries": "Journal",
    "notifications": "Notifications",
    "notification-push": "Notifications",
    "species-cache": "Catalog",
    "onboard-plant": "Garden",
    "species-sync": "Catalog",
}
"""The bounded context each consumer group serves, keyed by the group's suffix.

The groups are named after their job — ``garden``, ``notifications``,
``journal-entries`` — not after a context, so this mapping has to be explicit.
It is only used to label the generated diagram; the catalogue rows carry the group
names themselves, which is what ``docs/asyncapi-write.json`` subscribes with.
"""


class SchemaStub:
    """The part of a Dishka container a route registration needs.

    ``register_consumers`` and ``register_projections`` hand the container to a
    handler closure; FastStream only stores the callable, so generating a schema
    never opens a scope — and a schema generator has no business constructing a
    database engine to register routes it will not run. Anything that does reach
    for the container fails here, loudly, instead of quietly connecting.
    """

    async def __aenter__(self) -> SchemaStub:
        """Refuse: a schema generator must not open a request scope."""
        raise NotImplementedError("a schema generator must not open a request scope")

    async def __aexit__(self, *exc_info: object) -> bool:
        """Refuse: a schema generator must not open a request scope."""
        raise NotImplementedError("a schema generator must not open a request scope")


_DomainEventT = TypeVar("_DomainEventT", bound=DomainEvent)
"""The event type a projection's ``handlers`` mapping is keyed by."""

_HandlerT = TypeVar("_HandlerT", bound=object)
"""The callable type a projection's ``handlers`` mapping carries."""


class ProjectionLike(Protocol[_DomainEventT, _HandlerT]):
    """A read-side consumer, as the catalogue needs to see it.

    ``plantkeeper.admin``'s ``Projection`` satisfies this structurally, which is
    what lets this module describe the read side without importing Django: the
    admin's own adapter passes its projections in.

    Generic over the event and handler types because a projection declares
    ``handlers: ClassVar[Mapping[type[<concrete event>], <concrete handler>]]``,
    and a protocol naming ``object`` for either parameter would not match it: a
    ``ClassVar`` is invariant, and the mapping's parameters are checked as
    declared rather than by what is read out of it.
    """

    name: ClassVar[str]
    """The consumer group's suffix: ``<prefix>-<name>``."""

    handlers: ClassVar[Mapping[type[_DomainEventT], _HandlerT]]
    """The events this projection's handlers answer, keyed by event class."""


def event_context(event: type[DomainEvent]) -> str:
    """Return the bounded context that owns ``event``.

    ``plantkeeper.domain.<context>.events`` is the layout every context follows,
    so the third dotted segment names it.
    """
    return EVENT_CONTEXTS[event.__module__.split(".")[2]]


def producer_of(event: type[DomainEvent]) -> str:
    """Return the component that turns ``event`` into a Kafka message."""
    if event_context(event) == "Saga":
        return PRODUCER_SAGA_ENGINE
    if event is EVENT_TYPES["TelemetryReceived"]:
        return PRODUCER_TELEMETRY_INGRESS
    return PRODUCER_OUTBOX_RELAY


def _add_consumer(
    consumers: dict[type[DomainEvent], list[tuple[str, str]]],
    consumer_type: type[Consumer],
    label: str,
) -> None:
    """Record every event ``consumer_type`` handles, under ``label``."""
    for event in consumer_type.handled_types:
        consumers[event].append((label, consumer_type.name))


def write_side_consumers() -> dict[type[DomainEvent], list[tuple[str, str]]]:
    """Return ``event -> [(label, group suffix)]`` for the worker's consumers.

    A trigger is listed under the saga it starts, not under its own class name:
    the trigger is an implementation detail of the subscription, and the reader of
    the generated document is looking for the process manager.
    """
    consumers: dict[type[DomainEvent], list[tuple[str, str]]] = defaultdict(list)
    for consumer_type in CONSUMER_TYPES:
        _add_consumer(consumers, consumer_type, consumer_type.name)
    for trigger_type in TRIGGER_TYPES:
        saga = saga_type_named(trigger_type.saga_name) if trigger_type.saga_name else None
        label = (
            f"{saga.__name__} via {trigger_type.name}" if saga is not None else trigger_type.name
        )
        _add_consumer(consumers, trigger_type, label)
    return consumers


def read_side_consumers[EventT: DomainEvent, HandlerT: object](
    projections: Iterable[ProjectionLike[EventT, HandlerT]],
) -> dict[type[DomainEvent], list[tuple[str, str]]]:
    """Return ``event -> [(label, group suffix)]`` for the admin's projections."""
    consumers: dict[type[DomainEvent], list[tuple[str, str]]] = defaultdict(list)
    for projection in projections:
        for event in projection.handlers:
            consumers[event].append((f"{projection.name} projection", projection.name))
    return consumers


def catalogue_rows[EventT: DomainEvent, HandlerT: object](
    projections: Iterable[ProjectionLike[EventT, HandlerT]] = (),
) -> list[dict[str, object]]:
    """Return the event catalogue as JSON-ready rows, ordered by topic then name.

    ``projections`` is the read side's consumer list; a caller with Django
    configured passes ``ALL_PROJECTIONS``, and one without it gets the write
    side's view alone.
    """
    write_side = write_side_consumers()
    read_side = read_side_consumers(projections)
    rows: list[dict[str, object]] = []
    for event in sorted(EVENT_TOPICS, key=lambda item: (EVENT_TOPICS[item], item.__name__)):
        consumers = sorted({*write_side.get(event, ()), *read_side.get(event, ())})
        rows.append(
            {
                "event_name": event.__name__,
                "topic": EVENT_TOPICS[event],
                "context": event_context(event),
                "producer": producer_of(event),
                "consumers": [{"name": name, "consumer_group": group} for name, group in consumers],
                "payload_schema": f"#/components/schemas/{event.__name__}",
            }
        )
    return rows


def catalogue_extension[EventT: DomainEvent, HandlerT: object](
    projections: Iterable[ProjectionLike[EventT, HandlerT]] = (),
) -> dict[str, object]:
    """Return the AsyncAPI ``x-plantkeeper-event-catalogue`` extension object.

    The extension is what makes the document complete: AsyncAPI knows the
    channels the code subscribes to, while the producer half of every event is a
    database row the relay drains, and no FastStream route describes it.
    """
    return {
        "description": (
            "Every domain event, its topic, and the components that produce and "
            "consume it. Derived from plantkeeper.infrastructure.messaging.topics "
            "and the saga/consumer registries by `make contracts`."
        ),
        "events": catalogue_rows(projections),
    }


def event_flow_diagram[EventT: DomainEvent, HandlerT: object](
    projections: Iterable[ProjectionLike[EventT, HandlerT]] = (),
) -> str:
    """Return the Mermaid body of ``docs/diagrams/event-flow.md``.

    A *producer* edge is derived from the component that publishes (the relay for
    everything an aggregate recorded, the ingress for telemetry, the saga engine
    for the lifecycle events). A *consumer* edge is derived per event, from the
    consumer registries — both sides when ``projections`` is given.

    Only cross-context edges are drawn for consumers. A topic's events are mostly
    consumed by their own context — ``AdaptiveWateringSaga`` reads
    ``telemetry.events`` for Telemetry's own facts — and drawing those would turn
    the diagram into a list of the catalogue it is meant to summarise. Events that
    no cross-context consumer handles at all are called out in a note, because
    "nobody consumes this" is a fact a reader must not have to infer from silence.
    """
    rows = catalogue_rows(projections)
    topics = sorted({str(row["topic"]) for row in rows})
    producers: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    edges: dict[tuple[str, str], list[str]] = defaultdict(list)
    unconsumed: list[str] = []
    for row in rows:
        event = str(row["event_name"])
        producers[str(row["producer"])][str(row["topic"])].append(event)
        consumers = row["consumers"]
        assert isinstance(consumers, list)
        if not consumers:
            unconsumed.append(event)
        for consumer in consumers:
            assert isinstance(consumer, dict)
            context = _consumer_context(str(consumer["consumer_group"]))
            if context != str(row["context"]):
                edges[(event, context)].append(str(row["context"]))

    lines = ["```mermaid", "graph LR", "  subgraph produce [Producers]"]
    for producer, topics_of in sorted(producers.items()):
        for topic, events in sorted(topics_of.items()):
            lines.append(
                f'    {_producer_node(producer)} -->|"{", ".join(sorted(events))}"| {_node(topic)}'
            )
    for topic in topics:
        lines.append(f'    {_node(topic)}["{topic}"]')
    lines.extend(["  end", "  subgraph consume [Cross-context consumers]"])
    for (event, consumer_context), owners in sorted(edges.items()):
        owner = " / ".join(sorted(set(owners)))
        lines.append(f'    {_node(owner)} -->|"{event}"| {_node(consumer_context)}')
    lines.append("  end")
    if unconsumed:
        lines.append(f'  note["no cross-context consumer: {", ".join(sorted(unconsumed))}"]')
    lines.append("```")
    return "\n".join(lines) + "\n"


def _consumer_context(consumer_group: str) -> str:
    """Return the bounded context a consumer group serves, or ``Other``."""
    return CONSUMER_CONTEXTS.get(consumer_group, "Other")


def _producer_node(producer: str) -> str:
    """Return the Mermaid node id of a producer label."""
    if producer == PRODUCER_OUTBOX_RELAY:
        return "relay"
    if producer == PRODUCER_TELEMETRY_INGRESS:
        return "ingress"
    return "sagas"


def _node(label: str) -> str:
    """Return a Mermaid node id for a human-readable label.

    Node ids cannot contain dots, spaces or commas, and every label here does:
    topics are dotted, contexts and event lists are prose. The id is derived
    deterministically from the label, so two calls for the same label agree.
    """
    return "".join(character for character in label if character.isalnum())


def saga_diagrams() -> list[tuple[str, str]]:
    """Return ``(saga name, Mermaid sequence diagram)`` for the orchestration sagas.

    ``SagaMermaid`` reads a saga instance's ``steps``, which is why this builds one
    with a throwaway storage: the diagram is a property of the step list, so
    rendering it here is what keeps the checked-in diagram from describing a
    sequence the code no longer has.
    """
    diagrams: list[tuple[str, str]] = []
    for saga_type in SAGA_TYPES:
        saga = saga_type(_DiagramOnlyStorage())
        diagrams.append((saga_type.__name__, SagaMermaid(saga).sequence()))
    return diagrams


class _DiagramOnlyStorage:
    """A saga storage that is never used, for rendering a diagram.

    ``Saga.__init__`` stores the storage and does nothing else, and the diagram
    reads ``steps`` only. A stub keeps the generator free of a database session.
    """
