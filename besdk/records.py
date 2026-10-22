"""Sealing and opening security records as they travel (D76; P8.8): the canonical JSON bytes of a record without its
own digest, with the digest beside them. Opening checks, in order, that the bytes are canonical, that the digest is
theirs, and that the record passes its JSON Schema (be-protocol schemas/security, generated from the field tables);
the conditional rules of the tables stay with the callers.

The schemas are compiled once per record type with jsonschema-rs (Rust, draft 2020-12 natively, the dialect the
schemas are written in): opening a record spends 3-4% of its time validating, against 10-44% with fastjsonschema,
and the 24 security-record vectors are judged the same (the assembly project's experiment data 6b/sdk-jsonschema)."""
from __future__ import annotations

from typing import Any

import jsonschema_rs

from besdk import canon, protocol

MAJOR = 1


class Invalid(ValueError):
    """A record refused: not canonical, another digest, or against its schema."""


_validators: dict[str, jsonschema_rs.Draft202012Validator] = {}


def validate(record_type: str, v: Any) -> None:
    validator = _validators.get(record_type)
    if validator is None:
        try:
            s = protocol.schema(record_type)
        except FileNotFoundError:
            raise Invalid(f"no schema for record type {record_type}") from None
        validator = _validators[record_type] = jsonschema_rs.Draft202012Validator(s)
    try:
        validator.validate(v)
    except jsonschema_rs.ValidationError as e:
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
