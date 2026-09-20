"""The REST contract, generated without a running API process.

``docs/openapi.json`` is the same document FastAPI serves at ``/docs``; building it
through :func:`plantkeeper.api.main.create_app` means the test and the artefact can
only describe endpoints the application actually exposes.
"""

from __future__ import annotations

from plantkeeper.api.main import VERSION, create_app
from plantkeeper.api.openapi import build_document, render

API_PREFIX = "/api/v1"


def test_the_document_stamps_the_release_version() -> None:
    """``info.version`` is the contract version a client pins against."""
    document = build_document()
    info = document["info"]
    assert isinstance(info, dict)
    assert info["version"] == VERSION


def test_the_document_is_the_application_s_own_schema() -> None:
    """The generated document is what the app serves, not a second description."""
    assert build_document() == create_app().openapi()


def test_the_document_exposes_the_write_surface() -> None:
    """Every router of the write API contributes a path."""
    document = build_document()
    paths = document["paths"]
    assert isinstance(paths, dict)
    assert paths
    assert all(path.startswith(API_PREFIX) for path in paths)


def test_the_render_is_stable() -> None:
    """Two renders of the same document are byte-identical.

    The artefact is compared between runs — by a reviewer's diff and by
    ``tools/contracts.py --check`` — and FastAPI's own key order is not stable
    across calls, which is why :func:`render` sorts it.
    """
    document = build_document()
    assert render(document) == render(build_document())
    assert render(document).endswith("\n")
