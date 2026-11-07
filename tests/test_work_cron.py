"""Cron schedules against the protocol's vector cron-slots (P7.15, P7.16): slots in a time zone across the clock
changes, and what a materialization after a downtime produces under each backfill policy."""
import datetime as dt

import pytest

from besdk import protocol
from besdk.work import cron


def instant(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def z(t: dt.datetime) -> str:
    return t.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


CASES = protocol.vector("cron-slots")["cases"]


@pytest.mark.parametrize("c", [c for c in CASES if c["kind"] == "slots"], ids=lambda c: c["id"])
def test_slots(c):
    s = cron.parse(c["cron"], c["tz"])
    got = [{"slot": x.id, "at": z(x.at)} for x in s.slots(instant(c["after"]), instant(c["until"]))]
    assert got == c["expect"]


@pytest.mark.parametrize("c", [c for c in CASES if c["kind"] == "materialize"], ids=lambda c: c["id"])
def test_materialize(c):
    s = cron.parse(c["cron"], c["tz"])
    pending, paused, mark = cron.plan(s, c["policy"], c["max_backlog"], instant(c["watermark"]), instant(c["now"]))
    assert {"pending": [x.id for x in pending], "paused": [x.id for x in paused], "watermark": z(mark)} == c["expect"]


def test_every_case_kind_is_covered():
    assert {c["kind"] for c in CASES} == {"slots", "materialize"}


def test_next_after_and_bad_expressions():
    s = cron.parse("30 2 * * *", "Europe/Berlin")
    n = s.next_after(instant("2026-03-28T23:00:00Z"))
    assert (n.id, z(n.at)) == ("2026-03-29T02:30", "2026-03-29T01:00:00Z"), "the skipped 02:30 runs after the jump"
    assert cron.parse("0 0 31 2 *", "UTC").next_after(instant("2026-01-01T00:00:00Z")) is None, "no 31 February"
    for bad in ("* * * *", "60 * * * *", "* 24 * * *", "*/0 * * * *", "a * * * *", "5-1 * * * *"):
        with pytest.raises(ValueError):
            cron.parse(bad, "UTC")
    with pytest.raises(ValueError):
        cron.parse("* * * * *", "Mars/Olympus")


def test_none_skips_a_missed_slot_older_than_a_minute():
    # P7.16: backfill none runs a slot only if it came due within the last minute; the watermark moves past the
    # missed ones anyway (the vector's only none case is due within the minute, where none and latest agree)
    s = cron.parse("0 * * * *", "UTC")
    pending, paused, mark = cron.plan(s, "none", 24, instant("2026-10-17T06:00:30Z"), instant("2026-10-17T11:05:00Z"))
    assert (pending, paused, z(mark)) == ([], [], "2026-10-17T11:00:00Z")
