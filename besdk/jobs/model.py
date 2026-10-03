"""What a component declares for background work (be-protocol P14, apis §2.9 / §3): ``Job`` (every,
singleton, cron), ``Worker`` (queue), ``Reconciler``; and ``plan``: the declarations checked and merged with
``JOBS_OVERRIDES`` (P14.5) into the effective schedule of every job, runtime-owned ``be.*`` jobs included.
"""

from __future__ import annotations

import enum
import logging
import re
from dataclasses import dataclass, field
from datetime import tzinfo
from typing import Any, Awaitable, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from besdk.config_values import parse_duration_ns
from besdk.jobs.schedule import Schedule, ScheduleError

_NAME = re.compile(r"[a-z][a-z0-9_.-]*")
_KEYS = {"interval", "cron", "enabled"}


class JobsConfigError(ValueError):
    """An invalid declaration or override: the process exits 78 (P14.6)."""


class JobKind(enum.Enum):
    EVERY = "every"
    SINGLETON = "singleton"
    CRON = "cron"


@dataclass(frozen=True)
class Job:
    """``timeout`` (seconds) is required; ``interval`` for every / singleton; ``cron`` for cron jobs, in
    ``tz`` or else ``BUSINESS_TIMEZONE``."""

    name: str
    kind: JobKind
    timeout: float
    interval: float | None = None
    cron: str | None = None
    tz: str | None = None
    run: Callable[[], Awaitable[None]] | None = None


@dataclass(frozen=True)
class QueuedJob:
    id: str
    kind: str
    args: Any
    attempts: int
    max_attempts: int
    unique_key: str | None
    last_error: str = ""


@dataclass(frozen=True)
class Worker:
    """Executes queued jobs of ``kind`` at least once, outside any transaction; ``on_dead`` runs in a
    transaction when the attempts are exhausted."""

    kind: str
    run: Callable[[QueuedJob], Awaitable[None]]
    timeout: float
    max_attempts: int = 5
    concurrency: int = 1
    backoff: tuple[float, ...] = (1, 10, 60, 300, 900)
    on_dead: Callable[[Any, QueuedJob], Awaitable[None]] | None = None


@dataclass(frozen=True)
class Reconciler:
    """Drives in-flight processes past their deadline. ``candidates(tx, limit)`` is the component's SQL
    (non-terminal and past deadline); ``handle(item)`` runs outside any transaction and returns an outcome;
    ``apply(tx, item, outcome)`` advances the state machine in a short transaction; past ``max_attempts``
    failures ``give_up(tx, item)`` suspends it and opens an exception task."""

    name: str
    every: float
    timeout: float
    candidates: Callable[[Any, int], Awaitable[Sequence[Any]]]
    id: Callable[[Any], str]
    handle: Callable[[Any], Awaitable[Any]]
    apply: Callable[[Any, Any, Any], Awaitable[None]]
    batch: int = 100
    max_attempts: int = 10
    backoff: tuple[float, ...] = (10, 60, 300, 900, 3600)
    give_up: Callable[[Any, Any], Awaitable[None]] | None = None


# Runtime-owned jobs (P14.1): bound to their work by the runtime; listed so JOBS_OVERRIDES can name them.
CLEANUP = Job("be.cleanup", JobKind.CRON, timeout=300, cron="@every 1h")
OUTBOX = Job("be.outbox", JobKind.EVERY, timeout=60, interval=2.0)
LIFECYCLE = Job("be.lifecycle", JobKind.SINGLETON, timeout=300, interval=3600)
AUTHZ_CHANGES = Job("be.authz.changes", JobKind.SINGLETON, timeout=30, interval=5)
RUNTIME_NAMES = frozenset({"be.cleanup", "be.outbox", "be.lifecycle", "be.authz.changes"})


@dataclass(frozen=True)
class Planned:
    job: Job
    enabled: bool = True
    interval: float | None = None
    schedule: Schedule | None = None
    extra: dict = field(default_factory=dict)


def _zone(name: str | None, default: tzinfo) -> tzinfo:
    if not name:
        return default
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        raise JobsConfigError(f"unknown time zone {name!r}") from None


def _check(j: Job, seen: set[str]) -> None:
    if not _NAME.fullmatch(j.name or ""):
        raise JobsConfigError(f"job name {j.name!r} must match {_NAME.pattern}")
    if j.name in seen:
        raise JobsConfigError(f"job {j.name} is declared twice")
    if not j.timeout or j.timeout <= 0:
        raise JobsConfigError(f"job {j.name}: a timeout is required (P14.2)")
    if j.kind is JobKind.CRON and not j.cron:
        raise JobsConfigError(f"job {j.name}: a cron job needs cron")
    if j.kind is not JobKind.CRON and not (j.interval and j.interval > 0):
        raise JobsConfigError(f"job {j.name}: an {j.kind.value} job needs an interval")
    seen.add(j.name)


def _override(name: str, o: Any) -> dict:
    if not _NAME.fullmatch(name):
        raise JobsConfigError(f"JOBS_OVERRIDES: {name!r} is not a job name")
    if not isinstance(o, Mapping) or not o or set(o) - _KEYS:
        raise JobsConfigError(f"JOBS_OVERRIDES[{name}]: an object of interval, cron, enabled")
    out = dict(o)
    if "enabled" in out and not isinstance(out["enabled"], bool):
        raise JobsConfigError(f"JOBS_OVERRIDES[{name}].enabled is a boolean")
    if "interval" in out:
        ns = parse_duration_ns(out["interval"]) if isinstance(out["interval"], str) else None
        if not ns:
            raise JobsConfigError(f"JOBS_OVERRIDES[{name}].interval is a Go duration")
        out["interval"] = ns / 1e9
    if "cron" in out and not isinstance(out["cron"], str):
        raise JobsConfigError(f"JOBS_OVERRIDES[{name}].cron is a string")
    return out


def plan(jobs: Sequence[Job], overrides: Any, zone: tzinfo, logger: logging.Logger, *,
         runtime: Sequence[Job] = ()) -> dict[str, Planned]:
    """The effective jobs; raises JobsConfigError for anything invalid (exit 78)."""
    seen: set[str] = set()
    for j in jobs:
        if j.name.startswith("be."):
            raise JobsConfigError(f"job {j.name}: the prefix be. is the runtime's")
        _check(j, seen)
    for j in runtime:
        _check(j, seen)
    if overrides in (None, ""):
        overrides = {}
    if not isinstance(overrides, Mapping):
        raise JobsConfigError("JOBS_OVERRIDES is a JSON object keyed by job name")
    ov = {n: _override(n, o) for n, o in overrides.items()}
    out: dict[str, Planned] = {}
    for j in (*jobs, *runtime):
        o = ov.get(j.name, {})
        interval = o.get("interval", j.interval)
        sched = None
        if j.kind is JobKind.CRON:
            try:
                sched = Schedule.parse(o.get("cron", j.cron), _zone(j.tz, zone))
            except ScheduleError as e:
                raise JobsConfigError(f"job {j.name}: {e}") from None
        elif "cron" in o:
            raise JobsConfigError(f"JOBS_OVERRIDES[{j.name}].cron applies to cron jobs only")
        out[j.name] = Planned(j, o.get("enabled", True), interval, sched)
    for name in ov:
        if name not in out and name not in RUNTIME_NAMES:
            logger.warning("job_override_unknown", extra={"job": name})
    return out
