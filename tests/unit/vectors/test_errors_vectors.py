"""be-protocol vectors `errors` (P4, P10.4): codes, reasons, problem+json, SQLSTATE, log levels."""
import pytest

from besdk import errors as E
from tests.unit.vectors._load import cases, expect


@pytest.mark.parametrize("case", cases("errors", "codes", "grpc_to_http"))
def test_grpc_to_http(case):
    i = case["input"]

    def run():
        code = E.Code.parse(i["code"])
        return {"number": int(code), "http": E.http_status(code, i.get("reason"), i.get("domain"))}

    expect(case, run)


@pytest.mark.parametrize("case", cases("errors", "codes", "be_reason"))
def test_be_reason(case):
    def run():
        r = E.be_reason(case["input"]["reason"])
        return {"code": r.code.name, "domain": "be", "http": r.http}

    expect(case, run)


@pytest.mark.parametrize("case", cases("errors", "codes", "restore_http"))
def test_restore_http(case):
    i = case["input"]

    def run():
        err = E.restore_http(i["status"], i["problem"])
        return {"code": err.code.name, "reason": err.reason, "domain": err.domain, "http": err.http}

    expect(case, run)


@pytest.mark.parametrize("case", cases("errors", "sqlstate"))
def test_sqlstate_classify(case):
    i = case["input"]

    def run():
        m = i.get("component_mapping")
        mapped = E.Error(E.Code.parse(m["code"]), m["reason"], domain=m["domain"]) if m else None
        out = E.classify_sqlstate(i["sqlstate"], attempt=i.get("attempt", 1), context=i.get("context", "none"),
                                  mapped=mapped)
        if out.retry:
            return {"action": "retry", "base_delay_ms": out.base_delay_ms}
        err = out.error
        return {"action": "fail", "code": err.code.name, "reason": err.reason, "domain": err.domain,
                "http": err.http}

    expect(case, run)


@pytest.mark.parametrize("case", cases("errors", "levels"))
def test_log_level(case):
    expect(case, lambda: {"level": E.log_level(E.Code.parse(case["input"]["code"]))})


@pytest.mark.parametrize("case", cases("errors", "problem", "problem"))
def test_problem(case):
    i = case["input"]

    def run():
        e = i["error"]
        err = E.from_parts(code=e.get("code"), reason=e.get("reason"), domain=e.get("domain"),
                           metadata=e.get("metadata"), violations=e.get("violations"),
                           internal_message=e.get("internal_message"))
        p = E.problem(err, path=i["request"]["path"], request_id=i["request"]["request_id"],
                      trace_id=i["request"]["trace_id"], locale="en")
        body = {k: v for k, v in p.body.items() if k not in ("title", "detail")}
        return {"content_type": p.content_type, "body": body, "detail": p.body["detail"], "title": p.body["title"]}

    if "expected_error" in case:
        expect(case, run)
        return
    got = run()
    want = case["expected"]
    assert got["content_type"] == want["content_type"]
    assert got["body"] == want["body"]
    assert got["title"] and got["detail"]
    for s in want.get("detail_must_not_contain", []):
        assert s not in got["detail"]


@pytest.mark.parametrize("case", cases("errors", "problem", "retry_after"))
def test_retry_after(case):
    i = case["input"]
    expect(case, lambda: {"header": E.retry_after_header(E.Code.parse(i["code"]), i.get("retry_delay_ms"))})


@pytest.mark.parametrize("case", cases("errors", "problem", "reason_name"))
def test_reason_name(case):
    i = case["input"]

    def run():
        E.check_reason_name(i["reason"], i["domain"])
        return {"valid": True}

    expect(case, run)
