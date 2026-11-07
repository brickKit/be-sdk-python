"""The external commands' state table, resend deadline and parameters' digest (P13.2-P13.8), and the step 7 matching
(P13.12), against be-protocol's vectors external-command and external-reconcile."""
from datetime import datetime, timedelta, timezone

import pytest

from besdk import canon, protocol
from besdk.extcmd import reconcile, state
from besdk.extcmd.state import Capabilities, Event, Facts

KINDS = {"claim": state.CLAIM, "before_send": state.BEFORE_SEND, "lease_lost": state.LEASE_LOST,
         "send_result": state.SEND_RESULT, "finality_passed": state.FINALITY_PASSED, "registered": state.REGISTERED}
QUERY = {"found": state.QUERY_FOUND, "not_found": state.QUERY_NOT_FOUND, "unavailable": state.QUERY_DOWN}


def at(s):
    return None if s is None else datetime.fromisoformat(s.replace("Z", "+00:00"))


def fmt(t):
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def facts(c):
    return Facts(c.get("state", ""), at(c.get("resend_deadline")), at(c.get("expires_at")), at(c.get("first_reached_at")),
                 at(c.get("last_reached_at")), at(c.get("unknown_since")), at(c.get("accepted_at")),
                 at(c.get("unrecorded_until")))


def caps(c):
    return Capabilities(dedupe_window=timedelta(seconds=c.get("dedupe_window", 0)),
                        last_use=c.get("window_from") == "last_use", query=c.get("query", False),
                        max_processing=timedelta(seconds=c.get("max_processing", 0)))


@pytest.mark.parametrize("c", protocol.vector("external-command")["cases"], ids=lambda c: c["id"])
def test_the_external_command_vectors(c):
    i, want = c["input"], c["expected"]
    if want.get("state"):
        ev = i["event"]
        if ev["kind"] == "query":
            e = Event(QUERY[ev["result"]], external=ev.get("external_state", ""))
        else:
            e = Event(KINDS[ev["kind"]], result=ev.get("result", ""))
        assert state.step(facts(i["command"]), e, caps(i.get("capabilities", {})), at(i["now"])) == \
            (want["state"], want["reason"])
    elif want.get("digest"):
        assert canon.digest(state.PARAMS_RECORD, 1, i["incoming"]) == want["digest"]
    else:
        got = state.deadline(facts(i["command"]), caps(i.get("capabilities", {})))
        assert (None if got is None else fmt(got)) == want.get("resend_deadline")


@pytest.mark.parametrize("c", protocol.vector("external-reconcile")["cases"], ids=lambda c: c["id"])
def test_the_external_reconcile_vectors(c):
    i, want = c["input"], c["expected"]
    t, now = at(i["T"]), at(i["now"])
    until = at(i["failed_at"]) if i.get("failed_at") else now
    open_ = [reconcile.Open(o["reference"], "", "", facts(o), at(o.get("sending_at"))) for o in i["commands"]]
    cp = caps(i.get("capabilities", {}))
    got = reconcile.match(open_, i.get("external") or {}, set(), i.get("issued") or [], lambda op: cp, t, until, now)
    assert len(got) == len(want)
    for g in got:
        w = want[g.reference]
        assert (g.conclusion, g.state, g.detail) == (w["conclusion"], w.get("state", ""), w.get("reason", "")), g.reference
        assert (None if g.resend_deadline is None else fmt(g.resend_deadline)) == w.get("resend_deadline"), g.reference
