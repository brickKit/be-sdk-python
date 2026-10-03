"""Job declarations and JOBS_OVERRIDES (P14.1, P14.5, P14.6): names, required timeouts, overrides of
interval / cron / enabled for declared and runtime-owned jobs; unknown names warn; invalid values are
configuration errors."""
import logging
from zoneinfo import ZoneInfo

import pytest

from besdk.jobs.model import CLEANUP, OUTBOX, Job, JobKind, JobsConfigError, plan

TZ = ZoneInfo("Asia/Shanghai")


async def _noop():
    return None


def jobs():
    return [Job("widget.daily", JobKind.CRON, timeout=60, cron="0 3 * * *", run=_noop),
            Job("widget.sweep", JobKind.SINGLETON, timeout=10, interval=30, run=_noop),
            Job("widget.tick", JobKind.EVERY, timeout=5, interval=1, run=_noop)]


def test_plan_without_overrides():
    p = plan(jobs(), None, TZ, logging.getLogger("t"), runtime=(CLEANUP,))
    assert p["widget.daily"].schedule.text == "0 3 * * *" and p["widget.daily"].enabled
    assert p["widget.sweep"].interval == 30 and p["be.cleanup"].schedule.text == "@every 1h"


def test_overrides_apply_to_declared_and_runtime_jobs(caplog):
    o = {"widget.daily": {"enabled": False}, "widget.tick": {"interval": "200ms"},
         "be.cleanup": {"cron": "@every 2s"}, "nope": {"enabled": False}}
    with caplog.at_level(logging.WARNING):
        p = plan(jobs(), o | {"be.outbox": {"interval": "200ms"}}, TZ, logging.getLogger("t"),
                 runtime=(CLEANUP, OUTBOX))
    assert not p["widget.daily"].enabled and p["widget.tick"].interval == 0.2
    assert p["be.cleanup"].schedule.every_ns == 2_000_000_000
    assert p["be.outbox"].interval == 0.2
    assert "nope" not in p and any("job_override_unknown" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("bad", [{"widget.daily": {"cron": "@daily"}}, {"widget.daily": {}},
                                 {"widget.daily": {"enabled": "no"}}, {"widget.tick": {"interval": "fast"}},
                                 {"widget.daily": {"colour": "red"}}, {"Widget": {"enabled": False}}, ["x"]])
def test_invalid_overrides_are_configuration_errors(bad):
    with pytest.raises(JobsConfigError):
        plan(jobs(), bad, TZ, logging.getLogger("t"))


@pytest.mark.parametrize("bad", [
    Job("x", JobKind.CRON, timeout=0, cron="* * * * *", run=_noop),
    Job("x", JobKind.EVERY, timeout=1, run=_noop),
    Job("x", JobKind.CRON, timeout=1, cron="@hourly", run=_noop),
    Job("be.mine", JobKind.EVERY, timeout=1, interval=1, run=_noop),
    Job("Bad Name", JobKind.EVERY, timeout=1, interval=1, run=_noop),
])
def test_invalid_declarations(bad):
    with pytest.raises(JobsConfigError):
        plan([bad], None, TZ, logging.getLogger("t"))


def test_duplicate_names():
    with pytest.raises(JobsConfigError):
        plan(jobs() + jobs()[:1], None, TZ, logging.getLogger("t"))


def test_job_tz_overrides_business_zone():
    j = Job("w.ny", JobKind.CRON, timeout=1, cron="0 9 * * *", tz="America/New_York", run=_noop)
    assert plan([j], None, TZ, logging.getLogger("t"))["w.ny"].schedule.zone == ZoneInfo("America/New_York")
