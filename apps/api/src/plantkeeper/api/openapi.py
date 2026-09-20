"""The REST surface's OpenAPI document.

FastAPI already serves it at ``/docs`` on a running API process, but the document
is also an artefact worth having without one: a reviewer, a client generator or a
diff between two phases should not need Postgres, Kafka and a uvicorn to read the
contract. This module builds the same application object and asks it for the
schema, so nothing here can describe an endpoint the app does not expose.

The document is generated from the app a process would serve
(:func:`plantkeeper.api.main.create_app`) and includes its ``Idempotency-Key``
parameters, its request and response models, and its error responses — see
``docs/architecture.md`` for the write path the endpoints drive.

Run it with ``python -m plantkeeper.api.openapi --output docs/openapi.json`` (or
through ``make contracts``, which runs all three generators).
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from plantkeeper.api.main import VERSION, create_app


def build_document() -> dict[str, object]:
    """Return the OpenAPI document of the write API as a plain dictionary."""
    document = create_app().openapi()
    assert isinstance(document, dict)
    return document


def render(document: dict[str, object]) -> str:
    """Return the document as stable JSON text.

    FastAPI renders the same schema with a different key order per call, so the
    text is re-serialised with sorted keys: the artefact has to be comparable
    between runs for a diff — and ``--check`` — to mean anything.
    """
    return json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    """Write the REST contract to ``--output``."""
    parser = argparse.ArgumentParser(description="Export the write API's OpenAPI document.")
    parser.add_argument("--output", required=True, help="where to write the JSON document")
    arguments = parser.parse_args(argv)
    Path(arguments.output).write_text(render(build_document()), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["VERSION", "build_document", "main", "render"]
