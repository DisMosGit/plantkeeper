"""Generate the repository's checked-in and gitignored documentation artefacts.

One command renders everything that is derived from the code:

* ``docs/openapi.json`` — the REST contract, from the FastAPI application the API
  process serves;
* ``docs/asyncapi-read.json`` — Django Admin's Kafka subscriptions plus the
  platform-wide event catalogue, as an AsyncAPI 3.0 document;
* ``docs/asyncapi-write.json`` — the same for the worker, carrying the *same*
  catalogue rather than a write-side-only one;
* ``docs/diagrams/event-flow.md`` — the producer/consumer edge set of the event
  catalogue;
* ``docs/diagrams/sagas.md`` — the orchestration sagas' step sequences, rendered
  by ``python-cqrs``' own ``SagaMermaid``.

The generators run as child processes, and the order matters. Every process here
must import some application's code, and the read side is the only one that can
describe both consumer sets at once — its projections are consumers too — which
means importing Django. This process must not import Django, the worker's
generator must not either, so the admin generator runs first, writes the catalogue
into ``docs/asyncapi-read.json`` and the diagrams into ``docs/diagrams/``, and the
worker's generator is handed that document with ``--catalogue-from``. Without it
the write-side document would report every Django-projected event as consumed by
nobody.

The JSON artefacts are gitignored — they are build products, like the gRPC stubs
``tools/protogen.py`` writes — while the diagrams are checked in (a Mermaid block
in Markdown is readable in a diff, a JSON document is not). ``--check`` regenerates
everything into a temporary directory and compares, so a CI run or a reviewer can
ask "are the checked-in artefacts still true?" without writing to the tree.

Usage::

    uv run python tools/contracts.py             # write every artefact
    uv run python tools/contracts.py --check     # fail if anything is stale
    uv run python tools/contracts.py --diagrams-only
"""

from __future__ import annotations

import argparse
import filecmp
import importlib
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Sequence
from pathlib import Path
from typing import cast

ROOT = Path(__file__).resolve().parent.parent
DOCS_ROOT = ROOT / "docs"
DIAGRAMS_ROOT = DOCS_ROOT / "diagrams"

ROOT_PYPROJECT = ROOT / "pyproject.toml"

OPENAPI_MODULE = "plantkeeper.api.openapi"
OPENAPI_FILE = "openapi.json"

ADMIN_MODULE = "plantkeeper.admin.asyncapi"
ADMIN_FILE = "asyncapi-read.json"

WORKER_MODULE = "plantkeeper.workers.asyncapi"
WORKER_FILE = "asyncapi-write.json"

VERSION_MODULES: tuple[str, ...] = (OPENAPI_MODULE, ADMIN_MODULE, WORKER_MODULE)
"""The generators whose stamped version must match the workspace's release."""

JSON_ARTEFACTS: tuple[str, ...] = (OPENAPI_FILE, ADMIN_FILE, WORKER_FILE)
"""The generated JSON documents, in generation order."""


def project_version() -> str:
    """Return the version declared in the workspace's ``pyproject.toml``."""
    with ROOT_PYPROJECT.open("rb") as stream:
        document = tomllib.load(stream)
    version = document["project"]["version"]
    assert isinstance(version, str)
    return version


def check_version() -> int:
    """Fail when a generator's stamped version differs from the release.

    The constants are read from the generator modules rather than back out of the
    artefacts they wrote: the point is to catch a module whose literal drifted from
    the release, and a value read from its own output could never do that. Every
    module is imported by name here rather than at the top of this file, because
    importing the admin's one configures Django; the sub-process generators have
    run or are about to run anyway, so by this point it costs nothing.
    """
    version = project_version()
    mismatched = False
    for module_name in VERSION_MODULES:
        declared = cast("str", importlib.import_module(module_name).VERSION)
        if declared != version:
            print(
                f"error: {module_name} declares version {declared}, "
                f"but pyproject.toml says {version}",
                file=sys.stderr,
            )
            mismatched = True
    return 1 if mismatched else 0


def run_openapi(output: Path) -> None:
    """Write the REST contract."""
    subprocess.run(
        [sys.executable, "-m", OPENAPI_MODULE, "--output", str(output)],
        check=True,
        cwd=ROOT,
    )


def run_admin(output: Path, *, diagrams_root: Path) -> None:
    """Write the read side's document, and the platform's diagrams with it.

    One child process for both because both need Django configured and both are
    statements about every consumer in the platform, not only the read side's.
    """
    subprocess.run(
        [
            sys.executable,
            "-m",
            ADMIN_MODULE,
            "--output",
            str(output),
            "--diagrams-root",
            str(diagrams_root),
        ],
        check=True,
        cwd=ROOT,
    )


def run_worker(output: Path, *, catalogue_from: Path) -> None:
    """Write the write side's document, carrying the platform-wide catalogue."""
    subprocess.run(
        [
            sys.executable,
            "-m",
            WORKER_MODULE,
            "--output",
            str(output),
            "--catalogue-from",
            str(catalogue_from),
        ],
        check=True,
        cwd=ROOT,
    )


def generate(output_root: Path, *, diagrams_root: Path) -> None:
    """Run every generator, in the order their dependencies demand."""
    output_root.mkdir(parents=True, exist_ok=True)
    run_openapi(output_root / OPENAPI_FILE)
    run_admin(output_root / ADMIN_FILE, diagrams_root=diagrams_root)
    run_worker(output_root / WORKER_FILE, catalogue_from=output_root / ADMIN_FILE)


def check_contracts() -> int:
    """Return 0 when every artefact matches the code, 1 otherwise; write nothing."""
    if check_version() != 0:
        return 1
    stale: list[str] = []
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        diagrams_root = root / "diagrams"
        generate(root, diagrams_root=diagrams_root)
        stale.extend(
            name
            for name in JSON_ARTEFACTS
            if not filecmp.cmp(root / name, DOCS_ROOT / name, shallow=False)
        )
        stale.extend(
            path.name
            for path in _diagram_outputs(diagrams_root)
            if not filecmp.cmp(path, DIAGRAMS_ROOT / path.name, shallow=False)
        )
    if stale:
        print(
            "stale artefacts: "
            + ", ".join(sorted(stale))
            + "\nrun `make contracts` and commit the result",
            file=sys.stderr,
        )
        return 1
    print("contracts are up to date")
    return 0


def write_artefacts() -> int:
    """Write every artefact and report what was written."""
    if check_version() != 0:
        return 1
    generate(DOCS_ROOT, diagrams_root=DIAGRAMS_ROOT)
    for name in JSON_ARTEFACTS:
        print(f"wrote {(DOCS_ROOT / name).relative_to(ROOT)}")
    for path in _diagram_outputs(DIAGRAMS_ROOT):
        print(f"wrote {path.relative_to(ROOT)}")
    return 0


def write_diagrams() -> int:
    """Render only the diagrams, through the process that can describe both sides.

    It still writes the read-side document on the way: the diagram needs the
    catalogue in it, and regenerating a gitignored build product is cheaper than a
    second code path that builds the same view.
    """
    if check_version() != 0:
        return 1
    run_admin(DOCS_ROOT / ADMIN_FILE, diagrams_root=DIAGRAMS_ROOT)
    for path in _diagram_outputs(DIAGRAMS_ROOT):
        print(f"wrote {path.relative_to(ROOT)}")
    return 0


def _diagram_outputs(diagrams_root: Path) -> list[Path]:
    """Return the diagram files under ``diagrams_root``, in a stable order."""
    return sorted(diagrams_root.glob("*.md"))


def main(argv: Sequence[str] | None = None) -> int:
    """Write every artefact, or check the checked-in ones, and report."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="do not write; fail when a checked-in artefact is stale",
    )
    parser.add_argument(
        "--diagrams-only",
        action="store_true",
        help="render only the Mermaid diagrams, skipping the JSON contracts",
    )
    arguments = parser.parse_args(argv)

    if arguments.check:
        return check_contracts()
    if arguments.diagrams_only:
        return write_diagrams()
    return write_artefacts()


if __name__ == "__main__":
    raise SystemExit(main())
