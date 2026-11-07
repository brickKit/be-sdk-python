"""A subscription's progress against the protocol's vector inbox-progress (P6.37)."""
import pytest

from besdk import protocol
from besdk.events.progress import Handled, Point, SourceKey, progress_of

CASES = protocol.vector("inbox-progress")["cases"]


def key(name: str) -> SourceKey:
    return SourceKey("demo/source", "impl-1", "demo.stream", int(name[1:]))  # p0, p1


@pytest.mark.parametrize("c", CASES, ids=lambda c: c["id"])
def test_progress(c):
    i = c["input"]
    rows = [Handled(key(r["key"]), Point(*r["pos"]) if r["pos"] is not None else None, r["at"]) for r in i["rows"]]
    got = progress_of(i["runs"], rows, i["lifetime"])
    want = {key(k): Point(*v) for k, v in c["expected"]["progress"].items()}
    assert got == want
