"""The watermark source's check of a consumer's position against the protocol's vector watermark-pull-check (P9.15)."""
import pytest

from besdk import protocol
from besdk.events.watermark import RowIdentity, pull_check

CASES = protocol.vector("watermark-pull-check")["cases"]


def identity(r: dict | None) -> RowIdentity | None:
    if r is None:
        return None
    if "echo_nonce" in r:
        return RowIdentity(echo=r["echo_nonce"])
    rec = r["record"]
    return RowIdentity(record_id=rec["record_id"], version=rec["version"], digest=rec["digest"])


@pytest.mark.parametrize("c", CASES, ids=lambda c: c["id"])
def test_pull_check(c):
    src, req = c["source"], c["request"]
    pos = req.get("position")
    position = (pos["impl"], pos["epoch"], pos["seq"]) if pos else None
    at = identity(src["rows"].get(str(pos["seq"]))) if pos else None
    want = identity(req.get("row")) or RowIdentity()
    assert pull_check(src["impl"], src["epoch"], src["counter"], position, want, at) == c["expect"]
