"""Generate the repository's checked-in and gitignored documentation artefacts.

One command renders everything that is derived from the code:

* ``docs/openapi.json`` — the REST contract, from the FastAPI application the API
  process serves;
* ``docs/asyncapi-write.json`` — the worker's Kafka subscriptions plus the event
  catalogue, as an AsyncAPI 3.0 document;
* ``docs/asyncapi-read.json`` — the same for Django Admin's projections;
* ``docs/diagrams/event-flow.md`` — the producer/consumer edge set of the event
  catalogue;
* ``docs/diagrams/sagas.md`` — the orchestration sagas' step sequences, rendered
  by ``python-cqrs``' own ``SagaMermaid``.

The two AsyncAPI documents are produced by ``python -m`` in child processes on
purpose. The read side needs Django configured, the write side must not import it
at all (the layered-architecture contract in ``pyproject.toml``), and a process
that did both would be the one place where Django's configuration leaks into the
worker. A child process per document is the honest way to keep that boundary.

The JSON artefacts are gitignored — they are build products, like the gRPC stubs
``tools/protogen.py`` writes — while the diagrams are checked in (a Mermaid block
in Markdown is readable in a diff, a JSON document is not). ``--check`` regenerates
everything into a temporary directory and compares, so a CI run or a reviewer can
ask "are the checked-in diagrams still true?" without writing to the tree.

Usage::

    uv run python tools/contracts.py             # write every artefact
    uv run python tools/contracts.py --check     # fail if anything is stale
    uv run python tools/contracts.py --diagrams-only
"""

from __future__ import annotations

import argparse
import filecmp
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Sequence
from pathlib import Path

from plantkeeper.infrastructure.contracts.diagrams import render_diagrams

ROOT = Path(__file__).resolve().parent.parent
DOCS_ROOT = ROOT / "docs"
DIAGRAMS_ROOT = DOCS_ROOT / "diagrams"

ROOT_PYPROJECT = ROOT / "pyproject.toml"

CONTRACT_TARGETS: tuple[tuple[str, str], ...] = (
    ("plantkeeper.api.openapi", "openapi.json"),
    ("plantkeeper.workers.asyncapi", "asyncapi-write.json"),
    ("plantkeeper.admin.asyncapi", "asyncapi-read.json"),
)
"""The generated JSON artefacts: the module that writes each one, and its filename."""


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
    the release, and a value read from its own output could never do that. The read
    side is not imported — importing it configures Django, and this process must
    not be the one that does.
    """
    from plantkeeper.api.openapi import VERSION as API_VERSION
    from plantkeeper.workers.asyncapi import VERSION as WORKER_VERSION

    version = project_version()
    declared = {
        "plantkeeper.api.openapi": API_VERSION,
        "plantkeeper.workers.asyncapi": WORKER_VERSION,
    }
    for module, value in sorted(declared.items()):
        if value != version:
            print(
                f"error: {module} declares version {value}, but pyproject.toml says {version}",
                file=sys.stderr,
            )
    return 0 if all(value == version for value in declared.values()) else 1


def run_contract_generators(output_root: Path) -> list[Path]:
    """Run each process's contract module, writing its JSON under ``output_root``."""
    written: list[Path] = []
    for module, filename in CONTRACT_TARGETS:
        output = output_root / filename
        subprocess.run(
            [sys.executable, "-m", module, "--output", str(output)],
            check=True,
            cwd=ROOT,
        )
        written.append(output)
    return written


def stale_diagrams(diagrams: dict[Path, str]) -> list[str]:
    """Return the diagram filenames whose content on disk differs."""
    return [
        path.name
        for path, content in diagrams.items()
        if not path.exists() or path.read_text(encoding="utf-8") != content
    ]


def check_contracts(diagrams: dict[Path, str]) -> int:
    """Return 0 when every artefact matches the code, 1 otherwise; write nothing."""
    if check_version() != 0:
        return 1
    with tempfile.TemporaryDirectory() as temporary:
        output_root = Path(temporary)
        run_contract_generators(output_root)
        stale = [
            filename
            for _, filename in CONTRACT_TARGETS
            if not filecmp.cmp(output_root / filename, DOCS_ROOT / filename, shallow=False)
        ]
    stale.extend(stale_diagrams(diagrams))
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


def write_artefacts(diagrams: dict[Path, str], *, diagrams_only: bool) -> int:
    """Write the requested artefacts and report what was written."""
    written: list[Path] = []
    if not diagrams_only:
        if check_version() != 0:
            return 1
        DOCS_ROOT.mkdir(parents=True, exist_ok=True)
        written.extend(run_contract_generators(DOCS_ROOT))
    DIAGRAMS_ROOT.mkdir(parents=True, exist_ok=True)
    for path, content in diagrams.items():
        path.write_text(content, encoding="utf-8")
        written.append(path)
    for path in written:
        print(f"wrote {path.relative_to(ROOT)}")
    return 0


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

    diagrams = render_diagrams(DIAGRAMS_ROOT)
    if arguments.check:
        return check_contracts(diagrams)
    return write_artefacts(diagrams, diagrams_only=arguments.diagrams_only)


if __name__ == "__main__":
    raise SystemExit(main())
