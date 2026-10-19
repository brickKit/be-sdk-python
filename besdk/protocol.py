"""be-protocol's machine-readable parts as this SDK pins them (besdk/_protocol, copied by scripts/protocol.py)."""
import json
from importlib import resources
from importlib.resources.abc import Traversable

ROOT: Traversable = resources.files("besdk._protocol")


def pinned() -> dict:
    """The tag and commit of be-protocol the copy comes from."""
    return json.loads(ROOT.joinpath("PROTOCOL.json").read_text())


def vector(topic: str) -> dict:
    """One vector file, e.g. vector("errors")."""
    return json.loads(ROOT.joinpath("vectors", topic + ".json").read_text())


def migrations() -> Traversable:
    """The SDK's own migration files (chapter 05, P5.12)."""
    return ROOT.joinpath("migrations")


def capabilities() -> dict:
    """The capability list behind the feature matrix."""
    return json.loads(ROOT.joinpath("capabilities.json").read_text())


def schema(record_type: str) -> dict:
    """The JSON Schema of a security record type (schemas/security, generated from the field tables)."""
    return json.loads(ROOT.joinpath("schemas", "security", record_type + ".schema.json").read_text())
