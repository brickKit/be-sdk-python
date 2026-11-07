"""Cron schedules and their time slots (P7.15, P7.16; vector cron-slots).

A schedule is a 5-field expression (minute, hour, day of month, month, day of week; "*", lists, ranges, steps; 0 and 7
are both Sunday) in an IANA time zone. A slot's identity is its local date and time, YYYY-MM-DDTHH:MM. A local time
that does not exist (the clocks jump forward) runs at the first instant after the jump, its identity unchanged; a
local time that happens twice (the clocks go back) runs only the first time. When both the day of month and the day
of week are restricted, either one matching is enough (the cron convention)."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from besdk.work.job import ALL, LATEST, NONE

UTC = dt.UTC
SEARCH_DAYS = 5 * 366  # how far next_after looks before saying the schedule has no slot
NONE_WINDOW = dt.timedelta(seconds=60)  # backfill none: only a slot due this recently runs


@dataclass(frozen=True)
class Slot:
    id: str  # YYYY-MM-DDTHH:MM, local
    at: dt.datetime  # UTC


def _field(spec: str, lo: int, hi: int) -> set[int]:
    out: set[int] = set()
    for part in spec.split(","):
        step = 1
        if "/" in part:
            part, s = part.split("/", 1)
            if not s.isdigit() or int(s) < 1:
                raise ValueError(f"step {s!r}")
            step = int(s)
        if part == "*":
            a, b = lo, hi
        elif "-" in part:
            x, y = part.split("-", 1)
            if not (x.isdigit() and y.isdigit()):
                raise ValueError(f"range {part!r}")
            a, b = int(x), int(y)
        elif part.isdigit():
            a = int(part)
            b = hi if step > 1 else a  # "5/15" runs from 5 to the end
        else:
            raise ValueError(f"value {part!r}")
        if a < lo or b > hi or a > b:
            raise ValueError(f"{part!r} outside {lo}-{hi}")
        out.update(range(a, b + 1, step))
    return out


@dataclass(frozen=True)
class Schedule:
    minutes: frozenset[int]
    hours: frozenset[int]
    doms: frozenset[int]
    months: frozenset[int]
    dows: frozenset[int]  # 0 Sunday .. 6 Saturday
    dom_restricted: bool
    dow_restricted: bool
    tz: ZoneInfo

    def day_matches(self, d: dt.date) -> bool:
        if d.month not in self.months:
            return False
        dom, dow = d.day in self.doms, (d.isoweekday() % 7) in self.dows
        if self.dom_restricted and self.dow_restricted:
            return dom or dow
        return dom and dow

    def at(self, local: dt.datetime) -> dt.datetime:
        """The instant a local wall time runs at: its first occurrence; a time the clocks skipped, the first instant
        after the jump."""
        first = local.replace(tzinfo=self.tz, fold=0)
        u = first.astimezone(UTC)
        if u.astimezone(self.tz).replace(tzinfo=None) == local:
            return u
        t = local  # skipped: walk to the first local minute that exists
        for _ in range(24 * 60):
            t += dt.timedelta(minutes=1)
            u = t.replace(tzinfo=self.tz, fold=0).astimezone(UTC)
            if u.astimezone(self.tz).replace(tzinfo=None) == t:
                return u
        raise ValueError(f"no valid instant after {local} in {self.tz.key}")

    def _day(self, d: dt.date) -> list[Slot]:
        if not self.day_matches(d):
            return []
        return [Slot(f"{d.isoformat()}T{h:02d}:{m:02d}", self.at(dt.datetime(d.year, d.month, d.day, h, m)))
                for h in sorted(self.hours) for m in sorted(self.minutes)]

    def slots(self, after: dt.datetime, until: dt.datetime) -> list[Slot]:
        """The slots that run in (after, until], by instant (then identity)."""
        d, last = after.astimezone(self.tz).date() - dt.timedelta(days=1), until.astimezone(self.tz).date() + dt.timedelta(days=1)
        out = []
        while d <= last:
            out += [s for s in self._day(d) if after < s.at <= until]
            d += dt.timedelta(days=1)
        return sorted(out, key=lambda s: (s.at, s.id))

    def next_after(self, t: dt.datetime) -> Slot | None:
        """The first slot that runs after t; None when there is none within five years."""
        d = t.astimezone(self.tz).date() - dt.timedelta(days=1)
        for _ in range(SEARCH_DAYS):
            got = sorted((s for s in self._day(d) if s.at > t), key=lambda s: (s.at, s.id))
            if got:
                return got[0]
            d += dt.timedelta(days=1)
        return None

    def last_until(self, after: dt.datetime, until: dt.datetime) -> Slot | None:
        """The last slot in (after, until], walking back from until (a long downtime costs no more than one day)."""
        d, first = until.astimezone(self.tz).date() + dt.timedelta(days=1), after.astimezone(self.tz).date() - dt.timedelta(days=1)
        while d >= first:
            got = sorted((s for s in self._day(d) if after < s.at <= until), key=lambda s: (s.at, s.id))
            if got:
                return got[-1]
            d -= dt.timedelta(days=1)
        return None


def parse(expr: str, tz: str) -> Schedule:
    """Parses a 5-field expression in an IANA time zone; raises ValueError."""
    f = expr.split()
    if len(f) != 5:
        raise ValueError(f"cron {expr!r}: five fields")
    try:
        minutes, hours, doms, months = _field(f[0], 0, 59), _field(f[1], 0, 23), _field(f[2], 1, 31), _field(f[3], 1, 12)
        dows = {d % 7 for d in _field(f[4], 0, 7)}
    except ValueError as e:
        raise ValueError(f"cron {expr!r}: {e}") from None
    try:
        zone = ZoneInfo(tz)
    except Exception:  # noqa: BLE001 - any failure to load is a declaration error
        raise ValueError(f"time zone {tz!r}") from None
    return Schedule(frozenset(minutes), frozenset(hours), frozenset(doms), frozenset(months), frozenset(dows),
                    f[2] != "*", f[4] != "*", zone)


def plan(s: Schedule, backfill: str, max_backlog: int, watermark: dt.datetime,
         now: dt.datetime) -> tuple[list[Slot], list[Slot], dt.datetime]:
    """The slots due between the watermark and now, by the backfill policy: latest only the most recent; none only one
    due within the last minute; all the first max_backlog as pending and the rest paused (backlog_exceeded). The new
    watermark is the last due slot's instant (unchanged when none is due)."""
    if backfill == ALL:
        due = s.slots(watermark, now)
        if not due:
            return [], [], watermark
        return due[:max_backlog], due[max_backlog:], due[-1].at
    last = s.last_until(watermark, now)
    if last is None:
        return [], [], watermark
    if backfill == LATEST or (backfill == NONE and last.at > now - NONE_WINDOW):
        return [last], [], last.at
    return [], [], last.at
