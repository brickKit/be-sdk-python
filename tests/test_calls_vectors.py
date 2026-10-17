import json

from besdk import errs, errwire, protocol
from besdk.calls import policy
from besdk.calls.outcome import Result, classify, upward


def ms(n):
    return n / 1000


def test_outbound_timeout_vectors():
    for c in protocol.vector("outbound-timeout")["cases"]:
        rem = c.get("remaining_ms")
        t = policy.attempt_timeout(None if rem is None else ms(rem))
        assert (t is not None) == c["expect"]["send"], c["id"]
        if t is not None:
            assert abs(t - ms(c["expect"]["timeout_ms"])) < 1e-9, c["id"]
        else:
            assert c["expect"]["reason"] == "DEADLINE_BUDGET_EXHAUSTED", c["id"]


def test_retry_decision_vectors():
    for c in protocol.vector("retry-decision")["cases"]:
        if c.get("kind") == "backoff":
            assert abs(policy.max_backoff(c["failed_attempts"]) - ms(c["max_backoff_ms"])) < 1e-9, c["id"]
            continue
        after = c.get("remaining_ms_after_backoff")
        got = policy.should_retry(c["idempotent"], c["outcome"], c["attempts_made"], c["budget_allows"],
                                  None if after is None else ms(after))
        assert ("retry" if got else "stop") == c["expect"], c["id"]


def test_retry_budget_vectors():
    for c in protocol.vector("retry-budget")["cases"]:
        b = policy.Budget()
        for i, ev in enumerate(c["events"]):
            b.record(ev)
            assert b.tokens == c["expect_tokens"][i], (c["id"], i)
            assert b.allows_retry() == c["expect_retry_allowed"][i], (c["id"], i)
        assert b.allows_retry() == c["expect_final_retry_allowed"], c["id"]


def test_circuit_breaker_vectors():
    for c in protocol.vector("circuit-breaker")["cases"]:
        b, permits = policy.Breaker(), {}
        for i, s in enumerate(c["steps"]):
            at = 1000 + ms(s["at_ms"])
            if s["kind"] == "start":
                p = b.start(at)
                assert ("rejected" if p is None else "sent") == s["expect"], (c["id"], i)
                if p is not None:
                    permits[s["call"]] = p
            else:
                b.end(permits.get(s["call"], -1), at, s["outcome"])
            assert b.state == s["expect_state"], (c["id"], i)


def test_call_outcomes_vectors():
    for c in protocol.vector("call-outcomes")["cases"]:
        r = Result(connect_failed=c.get("connect_failed", False))
        remote = c.get("info")
        if c["surface"] == "grpc":
            r.code = c["code"]
        else:
            remote = c.get("problem")
            r.code = errwire.code_of_status(c.get("status") or 0)
            if remote and remote["domain"] == errs.PLATFORM_DOMAIN:
                r.code = errs.PLATFORM[remote["reason"]].code.value
        if remote:
            r.remote = errs.Error(remote["reason"], remote["domain"], errs.Code(r.code) if r.code != "OK" else errs.Code.UNKNOWN,
                                  0, "", "")
        if r.code != "OK" or r.connect_failed:
            r.err = RuntimeError("failed")
        cls = classify(r, c.get("caller_cancelled", False))
        e = c["expect"]
        assert (cls.value, cls.breaker, cls.budget, cls.retry_outcome) == (
            e["class"], e["breaker"], e["budget"], e["retry_outcome"]), c["id"]
        up = upward(cls, r, c.get("idempotent", False), c.get("dependency", ""))
        if e["upward"] is None:
            assert up is None, c["id"]
        elif e["upward"] == "unchanged":
            assert up is r.remote, c["id"]
        else:
            assert (up.domain, up.reason) == (e["upward"]["domain"], e["upward"]["reason"]), c["id"]
            for k, v in (e["upward"].get("metadata") or {}).items():
                assert up.metadata.get(k) == v, c["id"]
