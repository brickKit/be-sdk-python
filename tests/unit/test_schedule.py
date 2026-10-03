"""Schedules (P14.6): five-field cron in an IANA zone, or `@every <Go duration>` with slots at multiples
of the duration since the Unix epoch; anything else is a configuration error."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from besdk.jobs.schedule import Schedule, ScheduleError

UTC = timezone.utc
SH = ZoneInfo("Asia/Shanghai")
NY = ZoneInfo("America/New_York")


def u(*a):
    return datetime(*a, tzinfo=UTC)


def test_daily_cron_in_business_zone():
    s = Schedule.parse("0 3 * * *", SH)
    # 03:00 Shanghai = 19:00 UTC the day before
    assert s.next_after(u(2026, 10, 3, 12, 0)) == u(2026, 10, 3, 19, 0)
    assert s.prev_at_or_before(u(2026, 10, 3, 12, 0)) == u(2026, 10, 2, 19, 0)
    assert s.prev_at_or_before(u(2026, 10, 3, 19, 0)) == u(2026, 10, 3, 19, 0)


@pytest.mark.parametrize("expr,after,want", [
    ("*/15 * * * *", u(2026, 1, 1, 0, 7), u(2026, 1, 1, 0, 15)),
    ("0 9-17/4 * * 1-5", u(2026, 10, 3, 0, 0), u(2026, 10, 5, 9, 0)),  # Sat → Mon 09:00
    ("30 8 1,15 * *", u(2026, 10, 2, 0, 0), u(2026, 10, 15, 8, 30)),
    ("0 0 29 2 *", u(2026, 3, 1, 0, 0), u(2028, 2, 29, 0, 0)),
    ("0 12 * * 0", u(2026, 10, 3, 0, 0), u(2026, 10, 4, 12, 0)),  # Sunday as 0
    ("0 12 * * 7", u(2026, 10, 3, 0, 0), u(2026, 10, 4, 12, 0)),  # and as 7
    ("0 0 1 * 1", u(2026, 10, 2, 0, 0), u(2026, 10, 5, 0, 0)),  # dom OR dow when both are restricted
    ("0 0 * JAN,feb mon", u(2026, 10, 2, 0, 0), u(2027, 1, 4, 0, 0)),
])
def test_cron_fields(expr, after, want):
    assert Schedule.parse(expr, UTC).next_after(after) == want


def test_spring_forward_gap_is_skipped_and_fall_back_runs_once():
    s = Schedule.parse("30 2 * * *", NY)
    # 2026-03-08 02:30 does not exist in New York
    assert s.next_after(u(2026, 3, 8, 0, 0)) == datetime(2026, 3, 9, 2, 30, tzinfo=NY).astimezone(UTC)
    s = Schedule.parse("30 1 * * *", NY)
    first = s.next_after(u(2026, 11, 1, 0, 0))
    assert first == u(2026, 11, 1, 5, 30)  # 01:30 EDT
    assert s.next_after(first) == u(2026, 11, 2, 6, 30)  # not again at 01:30 EST


def test_every_slots_are_multiples_since_epoch():
    s = Schedule.parse("@every 90s", SH)
    assert s.next_after(u(2026, 1, 1, 0, 0, 1)) == u(2026, 1, 1, 0, 1, 30)
    assert s.prev_at_or_before(u(2026, 1, 1, 0, 1, 29)) == u(2026, 1, 1, 0, 0, 0)
    assert Schedule.parse("@every 1h30m", UTC).next_after(u(2026, 1, 1, 0, 0)) == u(2026, 1, 1, 1, 30)


@pytest.mark.parametrize("bad", ["", "@daily", "@reboot", "* * * *", "0 0 * * * *", "60 * * * *", "* 24 * * *",
                                 "* * 0 * *", "* * * 13 *", "* * * * 8", "@every 500ms", "@every 0s", "@every x",
                                 "*/0 * * * *", "5-1 * * * *", "a * * * *"])
def test_invalid_schedules(bad):
    with pytest.raises(ScheduleError):
        Schedule.parse(bad, UTC)
