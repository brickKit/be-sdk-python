"""The planning function against be-protocol's vector tier-plan (P15.37)."""
import datetime as dt

import pytest

from besdk import protocol
from besdk.tiering import plan

TIMES = ("anchor_max", "published_at", "newest_created")


def at(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


@pytest.mark.parametrize("c", protocol.vector("tier-plan")["cases"], ids=lambda c: c["id"])
def test_the_tier_plan_vectors(c):
    i, want = c["input"], c["expected"]
    k = i["class"]
    cls = plan.PlanClass(k["category"], plan.NEVER if k["residence_days"] == "never" else k["residence_days"],
                         k["retention"] == "pending", k["erasable"])
    unit = plan.PlanUnit(**{f: (at(v) if f in TIMES else v) for f, v in i["unit"].items()})
    got = plan.plan(at(i["now"]), dt.timedelta(days=i["n_days"]), dt.timedelta(days=i["grace_days"]), cls, unit)
    assert (got.action, got.reasons, got.alert) == (want["action"], want["reasons"], want.get("alert", False))
