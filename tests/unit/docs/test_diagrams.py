"""The generated Mermaid diagrams must still describe the code.

``docs/diagrams/*.md`` is checked in so GitHub renders it, and that is exactly why
it can go stale: a new saga step or a new consumer changes the diagram, and nothing
in the repository notices until a reader does. These tests regenerate both files
from the same functions ``make diagrams`` uses and compare them with what is
committed, so drift fails the build where a stale diagram would otherwise pass
review.
"""

from __future__ import annotations

from pathlib import Path

from plantkeeper.application.sagas.registry import SAGA_TYPES
from plantkeeper.infrastructure.contracts.catalogue import (
    EVENT_CONTEXTS,
    catalogue_rows,
    event_flow_diagram,
    saga_diagrams,
)
from plantkeeper.infrastructure.contracts.diagrams import render_diagrams
from plantkeeper.infrastructure.messaging.topics import EVENT_TOPICS

DIAGRAMS_ROOT = Path(__file__).resolve().parents[3] / "docs" / "diagrams"


def test_the_event_flow_diagram_matches_the_committed_file() -> None:
    """``docs/diagrams/event-flow.md`` is exactly what the catalogue renders."""
    rendered = render_diagrams(DIAGRAMS_ROOT)
    committed = (DIAGRAMS_ROOT / "event-flow.md").read_text(encoding="utf-8")
    assert committed == rendered[DIAGRAMS_ROOT / "event-flow.md"]


def test_the_saga_diagram_matches_the_committed_file() -> None:
    """``docs/diagrams/sagas.md`` is exactly what ``SagaMermaid`` renders."""
    rendered = render_diagrams(DIAGRAMS_ROOT)
    committed = (DIAGRAMS_ROOT / "sagas.md").read_text(encoding="utf-8")
    assert committed == rendered[DIAGRAMS_ROOT / "sagas.md"]


def test_every_saga_has_a_diagram() -> None:
    """No orchestration saga is left out of the generated diagram."""
    assert [name for name, _ in saga_diagrams()] == [saga.__name__ for saga in SAGA_TYPES]


def test_every_saga_diagram_names_its_real_steps() -> None:
    """A diagram's participants are the saga's own step classes, not prose.

    ``SagaMermaid`` truncates a participant longer than 30 characters, so the
    assertion is against the same rendering rule rather than the full class name.
    """
    for (_, body), saga_type in zip(saga_diagrams(), SAGA_TYPES, strict=True):
        for step in saga_type.steps:
            name = step.__name__
            rendered = name if len(name) <= 30 else f"{name[:27]}..."
            assert rendered in body


def test_the_event_flow_diagram_mentions_every_topic() -> None:
    """Each topic of the catalogue appears as a node in the diagram."""
    diagram = event_flow_diagram()
    for topic in {*EVENT_TOPICS.values()}:
        assert topic in diagram


def test_the_event_flow_diagram_mentions_every_cross_context_consumer() -> None:
    """A cross-context edge is drawn for every event that has one.

    The edge labels an event, and the note names the events nobody consumes
    across a context boundary; between the two, every event of the catalogue has
    to be accounted for in the diagram.
    """
    diagram = event_flow_diagram()
    for row in catalogue_rows():
        assert str(row["event_name"]) in diagram


def test_event_contexts_cover_every_event() -> None:
    """Every event resolves to a known context, so the diagram cannot KeyError."""
    assert {row["context"] for row in catalogue_rows()} <= {*EVENT_CONTEXTS.values()}
