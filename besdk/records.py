"""Sealing and opening security records as they travel (D76; P8.8): the canonical JSON bytes of a record without its
own digest, with the digest beside them. Opening checks, in order, that the bytes are canonical, that the digest is
theirs, and that the record passes its JSON Schema (be-protocol schemas/security, generated from the field tables);
the conditional rules of the tables stay with the callers.

The schemas are compiled once per record type with fastjsonschema: 15 to 45 times less CPU than the jsonschema
reference implementation (an audit record about 8 us against 364 us, a one-subject view about 140 us against
2.1 ms). It implements draft-07; the generated schemas use only keywords that mean the same there ($defs is reached
through ordinary JSON pointers), and the security-records vector checks it accepts and refuses what it must."""
from __future__ import annotations

from typing import Any, Callable

import fastjsonschema

from besdk import canon, protocol

MAJOR = 1


class Invalid(ValueError):
    """A record refused: not canonical, another digest, or against its schema."""


_validators: dict[str, Callable[[Any], Any]] = {}


def validate(record_type: str, v: Any) -> None:
    fn = _validators.get(record_type)
    if fn is None:
        try:
            s = protocol.schema(record_type)
        except FileNotFoundError:
            raise Invalid(f"no schema for record type {record_type}") from None
        s["$schema"] = "http://json-schema.org/draft-07/schema#"
        fn = _validators[record_type] = fastjsonschema.compile(s)
    try:
        fn(v)
    except fastjsonschema.JsonSchemaException as e:
        raise Invalid(f"{record_type}: {e.message}") from None


def seal(record_type: str, v: dict[str, Any]) -> tuple[bytes, str]:
    """A record for the wire: its canonical bytes and their digest. The record must not hold a digest."""
    raw = canon.encode(v)
    return raw, canon.digest_bytes(record_type, MAJOR, raw)


def open_(record_type: str, raw: bytes, digest: str) -> dict[str, Any]:
    """Checks a received record and returns it decoded."""
    try:
        v = canon.parse(raw)
    except canon.Refused as e:
        raise Invalid(str(e)) from None
    if canon.digest_bytes(record_type, MAJOR, raw) != digest:
        raise Invalid("the digest does not match the bytes")
    validate(record_type, v)
    if not isinstance(v, dict):
        raise Invalid("not an object")
    return v
