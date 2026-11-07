#!/usr/bin/env python3
"""Generates the protocol's gRPC code (be.source.v1, be.evidence.v1, be.external.v1, be.recovery.v1, be.session.v1)
from the protocol copy's proto/ into besdk/_gen, committed, as be-sdk-go commits its generated packages.

    uv run python scripts/gen.py          # regenerate
    uv run python scripts/gen.py --check  # the committed code is exactly what the pinned proto/ generates

The generated modules import each other as be.<package>; they are rewritten to besdk._gen.be.<package>, so the SDK's
code does not take the top-level name be from the components' contract packages (be.audit.v3, be.authz.v3)."""
import filecmp
import pathlib
import re
import shutil
import sys
import tempfile

from grpc_tools import protoc

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROTO = ROOT / "besdk" / "_protocol" / "proto"
OUT = ROOT / "besdk" / "_gen"
IMPORT = re.compile(r"^from be\.", re.M)


def generate(into: pathlib.Path) -> None:
    files = sorted(str(p.relative_to(PROTO)) for p in PROTO.rglob("*.proto"))
    include = pathlib.Path(protoc.__file__).parent / "_proto"  # google/protobuf/*.proto shipped with grpcio-tools
    if protoc.main(["protoc", f"-I{PROTO}", f"-I{include}", f"--python_out={into}", f"--pyi_out={into}",
                    f"--grpc_python_out={into}", *files]) != 0:
        sys.exit("protoc failed")
    for p in into.rglob("*.py*"):
        p.write_text(IMPORT.sub("from besdk._gen.be.", p.read_text()))
    for d in [into, *(p for p in into.rglob("*") if p.is_dir())]:
        (d / "__init__.py").write_text("")
    (into / "__init__.py").write_text('"""Generated from the protocol copy\'s proto/ by scripts/gen.py; never edited by hand."""\n')


def differences(a: pathlib.Path, b: pathlib.Path) -> list[str]:
    cmp = filecmp.dircmp(a, b, ignore=["__pycache__"])
    out = [f"only in {a}: {n}" for n in cmp.left_only] + [f"only in {b}: {n}" for n in cmp.right_only]
    out += [f"differs: {n}" for n in filecmp.cmpfiles(a, b, cmp.common_files, shallow=False)[1]]
    for sub in cmp.common_dirs:
        out += differences(a / sub, b / sub)
    return out


def main() -> None:
    if sys.argv[1:] == ["--check"]:
        with tempfile.TemporaryDirectory() as tmp:
            generate(pathlib.Path(tmp))
            if problems := differences(pathlib.Path(tmp), OUT):
                sys.exit("besdk/_gen is not what scripts/gen.py generates:\n  " + "\n  ".join(problems))
        print("generated code up to date")
        return
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir()
    generate(OUT)
    print(f"generated {len(list(OUT.rglob('*_pb2.py')))} modules into besdk/_gen")


if __name__ == "__main__":
    main()
