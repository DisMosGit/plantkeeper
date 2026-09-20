"""Generate the gRPC Python stubs from ``proto/``.

The stubs are not committed (``.gitignore`` keeps them out of the tree): they are
build products of the ``.proto`` files, and a committed copy would drift from the
contract it was generated from. ``make proto`` runs this script, and every target
that imports the stubs depends on it.

protoc derives a generated module's import path from the proto file's path
relative to the include root, so ``plantkeeper/v1/common.proto`` becomes
``plantkeeper.v1.common_pb2`` and the other generated modules import it as
``from plantkeeper.v1 import common_pb2``. The stubs themselves live under
``plantkeeper.api.grpc.generated``, so those absolute imports are rewritten to
that package path after generation. protoc has no option for this mapping
(``--python_opt=M...`` is rejected by ``grpc_tools.protoc``), and a runtime
``sys.modules`` alias would hide the indirection; a textual rewrite of a
generated file is the honest, small way to do it.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE_ROOT = ROOT / "proto"
GENERATED_ROOT = ROOT / "apps" / "api" / "src" / "plantkeeper" / "api" / "grpc" / "generated"
GENERATED_PACKAGE = "plantkeeper.api.grpc.generated"
GENERATED_SUFFIXES = (".py", ".pyi")


def proto_packages() -> list[str]:
    """Return the dotted proto package paths under ``proto/``.

    A package is any directory that contains a ``.proto`` file; the path is taken
    relative to ``proto/``, so ``proto/plantkeeper/v1`` becomes
    ``plantkeeper.v1``. Longest first, so a nested package is rewritten before the
    prefix it shares with its parent.
    """
    packages = {
        path.parent.relative_to(SOURCE_ROOT).as_posix().replace("/", ".")
        for path in SOURCE_ROOT.rglob("*.proto")
    }
    return sorted(packages, key=len, reverse=True)


def package_directories() -> list[Path]:
    """Return every directory under the generated root that must be a package.

    The generated root and the proto package directories inside it, plus their
    intermediate levels: a namespace package nested in a regular one is easy to
    get wrong, and an explicit ``__init__.py`` costs nothing.
    """
    directories = {GENERATED_ROOT}
    for package in proto_packages():
        directory = GENERATED_ROOT.joinpath(*package.split("."))
        directories.add(directory)
        directories.update(
            parent
            for parent in directory.parents
            if parent != GENERATED_ROOT and parent.is_relative_to(GENERATED_ROOT)
        )
    return sorted(directories)


def generate() -> int:
    """Run protoc over every ``.proto`` file, into a fresh output directory."""
    sources = sorted(SOURCE_ROOT.rglob("*.proto"))
    if not sources:
        raise SystemExit(f"no .proto files under {SOURCE_ROOT}")
    shutil.rmtree(GENERATED_ROOT, ignore_errors=True)
    for directory in package_directories():
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "__init__.py").touch()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"--proto_path={SOURCE_ROOT}",
            f"--python_out={GENERATED_ROOT}",
            f"--grpc_python_out={GENERATED_ROOT}",
            f"--pyi_out={GENERATED_ROOT}",
            *(str(source) for source in sources),
        ],
        check=True,
    )
    return sum(1 for path in GENERATED_ROOT.rglob("*") if path.suffix in GENERATED_SUFFIXES)


def rewrite_imports() -> None:
    """Point every generated import at the generated package."""
    for path in sorted(GENERATED_ROOT.rglob("*")):
        if path.suffix not in GENERATED_SUFFIXES:
            continue
        text = path.read_text(encoding="utf-8")
        for package in proto_packages():
            text = text.replace(
                f"from {package} import ",
                f"from {GENERATED_PACKAGE}.{package} import ",
            )
            text = text.replace(
                f"import {package}.",
                f"from {GENERATED_PACKAGE}.{package} import ",
            )
        path.write_text(text, encoding="utf-8")


def main() -> None:
    """Generate the stubs and report how many files were written."""
    written = generate()
    rewrite_imports()
    print(f"generated {written} gRPC stub file(s) in {GENERATED_ROOT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
