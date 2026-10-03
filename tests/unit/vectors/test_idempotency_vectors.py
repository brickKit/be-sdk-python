"""be-protocol vectors `idempotency` (P3.7, P13): fingerprints (RFC 8785 JCS + NFC + SHA-256), the decision
for an incoming command against the stored row, key resolution, caller namespaces and expiry."""
from datetime import datetime

import pytest

from besdk import idem
from tests.unit.vectors._load import cases, expect


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


@pytest.mark.parametrize("case", cases("idempotency", "fingerprint"))
def test_fingerprint(case):
    def run():
        canonical, digest = idem.fingerprint_text(case["input"]["json_text"])
        return {"canonical": canonical, "sha256": digest.hex()}

    expect(case, run)


def _caller(c: dict) -> str:
    return idem.caller_namespace(kind=c["kind"], sub=c.get("sub", ""), be_caller=c.get("be_caller", ""))


@pytest.mark.parametrize("case", cases("idempotency", "decide"))
def test_decide(case):
    i = case["input"]
    inc = i["incoming"]
    caller = _caller(inc["caller"])
    _, digest = idem.fingerprint_text(inc["request"])
    row = next((r for r in i["rows"] if r["caller"] == caller and r["idempotency_key"] == inc["key"]), None)
    stored = None if row is None else idem.Stored(command=row["command"], target=row["target"],
                                                  request_hash=bytes.fromhex(row["request_hash"]),
                                                  status=row["status"], result=row.get("result"),
                                                  expires_at=_ts(row["expires_at"]))
    d = idem.decide(stored, command=inc["command"], target=inc["target"], request_hash=digest, now=_ts(i["now"]))
    want = case["expected"]
    got = {"caller": caller, "outcome": d.outcome}
    if d.outcome == "REPLAY":
        got["result"] = d.result
    if d.outcome == "REJECT":
        got.update(code=d.error.code.name, http=d.error.http, reason=d.error.reason)
    assert got == want


@pytest.mark.parametrize("case", cases("idempotency", "keys", "resolve_key"))
def test_resolve_key(case):
    i = case["input"]
    if "expected_error" in case:
        with pytest.raises(Exception) as ei:
            idem.resolve_key(i["header"], i["body"])
        e = ei.value
        want = case["expected_error"]
        assert (e.reason, e.code.name, e.http) == (want["reason"], want["code"], want["http"])
        return
    assert {"key": idem.resolve_key(i["header"], i["body"])} == case["expected"]


@pytest.mark.parametrize("case", cases("idempotency", "keys", "caller_namespace"))
def test_caller_namespace(case):
    assert {"caller": _caller(case["input"]["caller"])} == case["expected"]


@pytest.mark.parametrize("case", cases("idempotency", "keys", "expires_at"))
def test_expiry(case):
    got = idem.expires_at(_ts(case["input"]["created_at"]))
    want = _ts(case["expected"]["expires_at"])
    assert got == want
