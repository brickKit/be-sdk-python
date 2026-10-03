"""Schedules of cron jobs (be-protocol P14.6): a five-field cron expression evaluated in an IANA zone, or
``@every <Go duration>`` (at least 1 s) whose slots are the multiples of the duration since the Unix epoch.

Cron fields: minute 0–59, hour 0–23, day of month 1–31, month 1–12 (or JAN…DEC), day of week 0–7 (or
SUN…SAT; 0 and 7 are Sunday); each a list of ``*``, ``n``, ``a-b``, each optionally ``/step``. When both
day fields are restricted a day matches either (classic cron). Wall times that do not exist in the zone
(spring forward) are skipped; a repeated wall time (fall back) runs once, at its first occurrence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone, tzinfo

from besdk.config_values import parse_duration_ns

UTC = timezone.utc
_MONTHS = {m: i + 1 for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split())}
_DAYS = {d: i for i, d in enumerate("SUN MON TUE WED THU FRI SAT".split())}
_HORIZON_DAYS = 366 * 9  # Feb 29 on a given weekday recurs within 28 years; 9 covers every plain case


class ScheduleError(ValueError):
    """An invalid schedule: a configuration error (exit 78)."""


def _field(text: str, lo: int, hi: int, names: dict[str, int] | None = None) -> frozenset[int]:
    out: set[int] = set()
    for part in text.split(","):
        rng, _, step_s = part.partition("/")
        step = _num(step_s, 1, hi - lo + 1, None) if step_s else 1
        if rng == "*":
            a, b = lo, hi
        elif "-" in rng:
            a_s, b_s = rng.split("-", 1)
            a, b = _num(a_s, lo, hi, names), _num(b_s, lo, hi, names)
            if a > b:
                raise ScheduleError(f"range {rng} is reversed")
        else:
            a = _num(rng, lo, hi, names)
            b = hi if step_s else a
        out.update(range(a, b + 1, step))
    return frozenset(out)


def _num(s: str, lo: int, hi: int, names: dict[str, int] | None) -> int:
    if names and s.upper() in names:
        return names[s.upper()]
    if not s.isdigit():
        raise ScheduleError(f"{s!r} is not a number")
    n = int(s)
    if not lo <= n <= hi:
        raise ScheduleError(f"{n} is outside {lo}–{hi}")
    return n


@dataclass(frozen=True)
class Schedule:
    text: str
    zone: tzinfo
    every_ns: int = 0
    minutes: frozenset[int] = frozenset()
    hours: frozenset[int] = frozenset()
    doms: frozenset[int] = frozenset()
    months: frozenset[int] = frozenset()
    dows: frozenset[int] = frozenset()
    dom_star: bool = True
    dow_star: bool = True

    @classmethod
    def parse(cls, text: str, zone: tzinfo) -> "Schedule":
        t = (text or "").strip()
        if t.startswith("@every "):
            ns = parse_duration_ns(t[7:].strip())
            if ns is None or ns < 1_000_000_000:
                raise ScheduleError(f"{text!r}: @every needs a Go duration of at least 1s")
            return cls(t, zone, every_ns=ns)
        f = t.split()
        if len(f) != 5:
            raise ScheduleError(f"{text!r}: a schedule is five cron fields or @every <duration>")
        try:
            dows = _field(f[4], 0, 7, _DAYS)
            return cls(t, zone, minutes=_field(f[0], 0, 59), hours=_field(f[1], 0, 23), doms=_field(f[2], 1, 31),
                       months=_field(f[3], 1, 12, _MONTHS), dows=frozenset(d % 7 for d in dows),
                       dom_star=f[2] == "*", dow_star=f[4] == "*")
        except ScheduleError as e:
            raise ScheduleError(f"{text!r}: {e}") from None

    # --- slots ---------------------------------------------------------------------------------

    def next_after(self, t: datetime) -> datetime:
        """The first slot strictly after ``t`` (UTC)."""
        if self.every_ns:
            n = _ns(t) // self.every_ns + 1
            return _at(n * self.every_ns)
        return self._scan(t, forward=True)

    def prev_at_or_before(self, t: datetime) -> datetime:
        """The latest slot at or before ``t`` (UTC): the slot a catch-up or ``job run`` claims."""
        if self.every_ns:
            return _at(_ns(t) // self.every_ns * self.every_ns)
        return self._scan(t, forward=False)

    def _day_matches(self, d: date) -> bool:
        if d.month not in self.months:
            return False
        dom, dow = d.day in self.doms, d.isoweekday() % 7 in self.dows
        if self.dom_star or self.dow_star:
            return dom and dow
        return dom or dow

    def _scan(self, t: datetime, *, forward: bool) -> datetime:
        start = t.astimezone(self.zone).date()
        step = 1 if forward else -1
        hours, minutes = sorted(self.hours, reverse=not forward), sorted(self.minutes, reverse=not forward)
        for i in range(-1 if forward else 1, _HORIZON_DAYS * step, step):
            d = start + timedelta(days=i)
            if not self._day_matches(d):
                continue
            for h in hours:
                for m in minutes:
                    slot = _wall(d, h, m, self.zone)
                    if slot is not None and (slot > t if forward else slot <= t):
                        return slot
        raise ScheduleError(f"{self.text!r} never fires")


def _wall(d: date, h: int, m: int, zone: tzinfo) -> datetime | None:
    """The UTC instant of a wall time in ``zone``: None when it does not exist; the first when repeated."""
    local = datetime.combine(d, time(h, m), tzinfo=zone)
    utc = local.astimezone(UTC)
    if utc.astimezone(zone).replace(tzinfo=None) != local.replace(tzinfo=None):
        return None
    return utc


def _ns(t: datetime) -> int:
    t = t.astimezone(UTC)
    return (int(t.replace(microsecond=0).timestamp()) * 1_000_000 + t.microsecond) * 1000


def _at(ns: int) -> datetime:
    return datetime.fromtimestamp(ns // 1_000_000_000, UTC) + timedelta(microseconds=ns % 1_000_000_000 // 1000)
