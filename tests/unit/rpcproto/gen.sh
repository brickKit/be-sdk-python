#!/bin/bash
# Regenerate the test service's code (needs grpcio-tools and be-protocol's limits.proto in ../../..).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../../.." && pwd)
PY=${PY:-$ROOT/.venv/bin/python}
INC=$("$PY" -c 'import grpc_tools, os; print(os.path.join(os.path.dirname(grpc_tools.__file__), "_proto"))')
PROTO=${BE_PROTOCOL_PROTO:-$ROOT/../be-protocol/proto}
"$PY" -m grpc_tools.protoc -I "$HERE" -I "$PROTO" -I "$INC" --python_out="$HERE" --grpc_python_out="$HERE" \
  conformance/peer/v1/peer.proto
touch "$HERE/conformance/__init__.py" "$HERE/conformance/peer/__init__.py" "$HERE/conformance/peer/v1/__init__.py"
