"""The write side's AsyncAPI document.

One FastStream broker, pointed at the configured bootstrap servers but never
started, with the same routes ``python -m plantkeeper.workers`` registers. The
document is therefore a fact about the worker's subscriptions rather than a
description of them: adding a consumer to the registry changes the document.

Two things a generated AsyncAPI document cannot say on its own, and this module
adds:

* **the producers.** The worker subscribes to topics; it does not publish to them.
  Every write-side event is published by ``OutboxRelay`` draining
  ``write_shared.outbox``, which is not a FastStream route at all.
* **the event catalogue.** Which event travels on which topic, and which consumer
  group handles it, lives in ``EVENT_TOPICS`` and the saga registry.

Both arrive as the ``x-plantkeeper-event-catalogue`` extension
(:mod:`plantkeeper.infrastructure.contracts.catalogue`), so the document a reader
opens is complete even though half the system is not an AsyncAPI route.

Run it with ``python -m plantkeeper.workers.asyncapi --output docs/asyncapi-write.json``
(or through ``make contracts``, which runs all three generators).
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from dishka import AsyncContainer
from faststream.kafka import KafkaBroker
from faststream.specification.asyncapi import AsyncAPI

from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.contracts.catalogue import SchemaStub, catalogue_extension
from plantkeeper.workers.consumers import register_consumers, register_telemetry_ingest

TITLE = "PlantKeeper write-side events"
"""The document's title, fixed so two runs produce the same bytes."""

VERSION = "0.1.0"
"""The contract version, kept in step with the workspace's ``pyproject.toml``.

The release tag is what a consumer of this document cares about, so
``tools/contracts.py`` fails the run if the two ever disagree.
"""

DESCRIPTION = (
    "Kafka subscriptions of `plantkeeper.workers` — the outbox relay's consumers, "
    "the telemetry ingress, the choreography sagas and the orchestration triggers "
    "— plus the domain-event catalogue the relay produces."
)


def build_broker(settings: Settings) -> KafkaBroker:
    """Return a broker with every write-side route registered."""
    broker = KafkaBroker(settings.kafka_bootstrap_servers)
    # Only route registration happens here, so the stub never opens a scope; it
    # fails loudly if a future change makes one of the registrars reach for it.
    container = cast("AsyncContainer", SchemaStub())
    register_consumers(broker, container=container, settings=settings)
    register_telemetry_ingest(broker, container=container, settings=settings)
    return broker


def build_document(settings: Settings) -> dict[str, object]:
    """Return the AsyncAPI document as a plain, JSON-ready dictionary."""
    specification = AsyncAPI(
        build_broker(settings),
        title=TITLE,
        version=VERSION,
        description=DESCRIPTION,
    ).to_specification()
    document = specification.to_jsonable()
    assert isinstance(document, dict)
    document["x-plantkeeper-event-catalogue"] = catalogue_extension()
    return document


def render(document: dict[str, object]) -> str:
    """Return the document as stable JSON text.

    Sorted keys and a trailing newline, because the artefact is compared between
    runs (and by ``--check``): a dictionary that serialises differently on every
    run would make the comparison meaningless.
    """
    return json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    """Write the write side's AsyncAPI document to ``--output``."""
    parser = argparse.ArgumentParser(description="Export the write side's AsyncAPI document.")
    parser.add_argument("--output", required=True, help="where to write the JSON document")
    arguments = parser.parse_args(argv)
    Path(arguments.output).write_text(render(build_document(Settings())), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
