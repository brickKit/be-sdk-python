"""The evidence record's canonical form against the protocol's vectors (P12.9; position-by-phase is checked on the
records the writer really writes, test_evidence_integration), an intent the canonical form cannot encode (P12.16), and
one way to replay a category (P12.20)."""
import asyncio
import io
import json

import pytest

from besdk import canon, obs, protocol
from besdk.evidence import Object, Replays, Writer, seal


def cases():
    return {c["id"]: c for c in protocol.vector("evidence-record")["cases"]}


@pytest.mark.parametrize("case", ["digest-intent", "digest-effective", "digest-cancelled", "digest-general"])
def test_the_evidence_digest_vectors(case):
    c = cases()[case]
    s = seal(c["input"]["record"])
    assert s.digest == c["expected"]["digest"]
    assert json.loads(s.record) == c["input"]["record"], "the canonical bytes are the record"


def test_an_intent_that_cannot_be_encoded_is_logged():  # P12.16
    out = io.StringIO()
    w = Writer(None, "demo/orders", "orders-1", obs.new_logger(obs.Member("demo/orders", "3.0.0"), "info", out))
    with pytest.raises(canon.Refused):
        asyncio.run(w.intent("security_tightening", "", Object("demo.order", "o1"), {"ratio": 0.5}))
    line = json.loads(out.getvalue().splitlines()[-1])
    assert line["event"] == "evidence_intent_unsealed" and line["category"] == "security_tightening"
    assert line["object"] == "o1"


def test_a_category_is_replayed_one_way():  # P12.20
    async def replay(tx, r):
        return None

    async def check(r):
        return True, ""

    r = Replays()
    r.add("credential_tightening", replay)
    with pytest.raises(ValueError, match="registered twice"):
        r.check("credential_tightening", check)
    ok = Replays()
    ok.add("a", replay)
    ok.check("b", check)
