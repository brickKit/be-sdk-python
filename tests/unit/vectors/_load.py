"""Load be-protocol vector files (copied by `make sync-protocol`) as pytest parameters."""
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2] / "protocol"


def cases(area: str, topic: str, *ops: str) -> list:
    """One pytest.param per case of vectors/<area>/<topic>.json, optionally only the given ops."""
    data = json.loads((ROOT / "vectors" / area / f"{topic}.json").read_text())
    return [pytest.param(c, id=c["id"]) for c in data["cases"] if not ops or c["op"] in ops]


def expect(case: dict, fn):
    """Run fn(); compare with expected, or check the error reason against expected_error."""
    from besdk.errors import ProtocolError

    if "expected_error" in case:
        with pytest.raises(ProtocolError) as ei:
            fn()
        assert ei.value.reason == case["expected_error"]["reason"]
        return None
    got = fn()
    assert got == case["expected"]
    return got
