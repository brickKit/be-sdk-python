"""Batch limits on repeated fields (be-protocol P7.10): ``(be.v1.max_items)``, default 500 (repro r1-03b)."""

from __future__ import annotations

from functools import cache

from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import Message

from be.v1 import limits_pb2

DEFAULT = 500


def _repeated(f: FieldDescriptor) -> bool:
    try:
        return bool(f.is_repeated)
    except AttributeError:
        return f.label == FieldDescriptor.LABEL_REPEATED


def _is_map(f: FieldDescriptor) -> bool:
    return f.message_type is not None and f.message_type.GetOptions().map_entry


@cache
def table(desc: Descriptor) -> tuple[tuple[str, str, int | None, Descriptor | None], ...]:
    """(field name, path prefix-free name, limit or None for a nested message, nested descriptor)."""
    out = []
    for f in desc.fields:
        if _repeated(f) and not _is_map(f):
            opts = f.GetOptions()
            limit = opts.Extensions[limits_pb2.max_items] if opts.HasExtension(limits_pb2.max_items) else DEFAULT
            out.append((f.name, f.name, limit, None))
        elif f.type == FieldDescriptor.TYPE_MESSAGE and not _repeated(f):
            out.append((f.name, f.name, None, f.message_type))
    return tuple(out)


def check(msg: Message, prefix: str = "") -> tuple[str, int, int] | None:
    """The first repeated field above its limit: (field path, max, got); None when within limits."""
    for name, _, limit, nested in table(msg.DESCRIPTOR):
        if limit is not None:
            got = len(getattr(msg, name))
            if got > limit:
                return prefix + name, limit, got
        elif msg.HasField(name):
            hit = check(getattr(msg, name), f"{prefix}{name}.")
            if hit:
                return hit
    return None
