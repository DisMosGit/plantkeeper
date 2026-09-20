"""The read side's AsyncAPI document.

The admin process is the other Kafka consumer group in the platform: five
projections, one per read model, each subscribed to the topics its events travel
on. It gets a document of its own rather than sharing the worker's because the two
processes may not import each other (the layered-architecture contract in
``pyproject.toml``), and because their subscriptions genuinely differ — a reader
asking "what feeds Django Admin?" should not have to filter the write side's sagas
out of the answer.

Django has to be configured before a projection can be imported, and
:func:`main` does that itself: ``django.setup()`` loads the app registry without
opening a database connection, which is what keeps this usable on a machine with
no Postgres running.

Run it with ``python -m plantkeeper.admin.asyncapi --output docs/asyncapi-read.json``
(or through ``make contracts``, which runs all three generators).
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Final

from faststream.kafka import KafkaBroker
from faststream.specification.asyncapi import AsyncAPI

from plantkeeper.infrastructure.config import Settings
from plantkeeper.infrastructure.contracts.catalogue import catalogue_extension

TITLE = "PlantKeeper read-side events"
"""The document's title, fixed so two runs produce the same bytes."""

VERSION = "0.1.0"
"""The contract version; ``tools/contracts.py`` checks it against ``pyproject.toml``."""

DESCRIPTION = (
    "Kafka subscriptions of `plantkeeper.admin` — the five projections that keep "
    "the `read_analytics` read models in step with the write side's events."
)

SETTINGS_MODULE: Final = "plantkeeper.admin.settings"


def configure_django() -> None:
    """Configure Django unless the caller already did.

    Importing a projection imports its read model, which imports Django's model
    machinery, so this has to run first. Nothing connects to the database during
    ``setup()``: the app registry is built from the settings and the ``apps``
    package, and a projection's queries only run when it is actually applied.
    """
    from django.apps import apps

    if apps.apps_ready:
        return
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", SETTINGS_MODULE)
    os.environ.setdefault("DJANGO_DEBUG", "true")

    import django

    django.setup()


def build_document(settings: Settings) -> dict[str, object]:
    """Return the read side's AsyncAPI document as a plain, JSON-ready dictionary."""
    configure_django()
    from plantkeeper.admin.projections import ALL_PROJECTIONS
    from plantkeeper.admin.projections.subscriber import register_projections

    broker = KafkaBroker(settings.kafka_bootstrap_servers)
    register_projections(broker, prefix=settings.read_side_consumer_group_prefix)
    specification = AsyncAPI(
        broker,
        title=TITLE,
        version=VERSION,
        description=DESCRIPTION,
    ).to_specification()
    document = specification.to_jsonable()
    assert isinstance(document, dict)
    # The read side sees both halves: it consumes the catalogue's events, and the
    # producers are the same components the write side's document names.
    document["x-plantkeeper-event-catalogue"] = catalogue_extension(ALL_PROJECTIONS)
    return document


def render(document: dict[str, object]) -> str:
    """Return the document as stable JSON text; see the worker's twin."""
    return json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    """Write the read side's AsyncAPI document to ``--output``."""
    parser = argparse.ArgumentParser(description="Export the read side's AsyncAPI document.")
    parser.add_argument("--output", required=True, help="where to write the JSON document")
    arguments = parser.parse_args(argv)
    Path(arguments.output).write_text(render(build_document(Settings())), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["TITLE", "VERSION", "build_document", "main", "render"]
