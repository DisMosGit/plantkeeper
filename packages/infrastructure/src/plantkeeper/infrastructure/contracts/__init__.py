"""Generated documentation artefacts.

The project's integration contract has four faces: the REST surface's OpenAPI
document, the Kafka surface's two AsyncAPI documents, and the Mermaid diagrams of
the event topology and the orchestration sagas. All of them are build products, not
sources: they are derived from the code that implements them, so a committed copy
would drift.

This package holds the derivations themselves — :mod:`catalogue` is the shared half
that every document and every diagram is built from, and :mod:`diagrams` renders
the two checked-in files. The per-process adapters live next to the processes they
describe (``plantkeeper.api.openapi``, ``plantkeeper.workers.asyncapi``,
``plantkeeper.admin.asyncapi``), because generating a FastAPI or a Django document
means importing FastAPI or Django, and neither belongs in this layer.

``tools/contracts.py`` runs all of it; the tests in ``tests/unit/docs`` call the
same functions and compare the result with what is committed.
"""

from __future__ import annotations

__all__: tuple[str, ...] = ()
