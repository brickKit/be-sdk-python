#!/bin/bash
# Generate be/v1/limits_pb2.py (top-level package `be`) from be-protocol's proto/be/v1/limits.proto
# at the pinned tag (P7.10, repro r1-03b). The generated file is committed and ships in the wheel,
# so a component's generated code (`from be.v1 import limits_pb2`) links to this one copy.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TAG=${1:-v1.0.0-rc.1}
REPO=${BE_PROTOCOL_REPO:-https://github.com/brickKit/be-protocol}
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
(cd "$ROOT" && git -c advice.detachedHead=false clone -q --depth 1 --branch "$TAG" "$REPO" "$TMP/p")
PY=${PY:-$ROOT/.venv/bin/python}
INC=$("$PY" -c 'import grpc_tools, os; print(os.path.join(os.path.dirname(grpc_tools.__file__), "_proto"))')
mkdir -p "$ROOT/be/v1"
"$PY" -m grpc_tools.protoc -I "$TMP/p/proto" -I "$INC" --python_out="$ROOT" --pyi_out="$ROOT" be/v1/limits.proto
touch "$ROOT/be/__init__.py" "$ROOT/be/v1/__init__.py"
echo "generated be/v1/limits_pb2.py from be-protocol $TAG"
