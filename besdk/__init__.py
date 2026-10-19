"""The official Python SDK of the component protocol (be-protocol)."""
import os
from importlib import metadata

# Version is this SDK's version, reported in the self-description (protocol P1.8): read from the installed package, so
# it cannot drift from pyproject.toml (a constant here once stayed at 3.0.0-alpha.9 for eight releases).
Version = metadata.version("be-sdk")

# gRPC's C core writes its own plain-text lines to stderr (TLS handshake notes and the like): they would break "one
# JSON object per line" (P10.1), and the SDK reports every call itself. Set before any SDK module imports grpc; an
# operator who sets GRPC_VERBOSITY keeps their value.
os.environ.setdefault("GRPC_VERBOSITY", "NONE")
