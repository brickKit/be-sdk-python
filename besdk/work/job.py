"""A job's declaration (protocol chapter 07; the assembly project's protocol 01): its kind, its effect class, its
limits and handlers; the kinds of failure a handler reports; the backoff."""
from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from besdk.db.pool import Tx
    from besdk.work.run import Run

# Kinds (protocol 01's four kinds of work).
QUEUE, CRON, SINGLETON, EVERY = "queue", "cron", "singleton", "every"
KINDS = (QUEUE, CRON, SINGLETON, EVERY)

# Effect classes (P7.4): declared, never inferred at run time.
LOCAL, IDEMPOTENT_EXTERNAL, NON_IDEMPOTENT_EXTERNAL, EMIT_ONLY = (
    "local", "idempotent_external", "non_idempotent_external", "emit_only")
EFFECTS = (LOCAL, IDEMPOTENT_EXTERNAL, NON_IDEMPOTENT_EXTERNAL, EMIT_ONLY)

# Backfill policies of cron jobs (P7.16).
LATEST, ALL, NONE = "latest", "all", "none"

# Parameters of protocol 01.
DEFAULT_LEASE = 30.0
DEFAULT_MAX_ATTEMPTS = 10
# The attempt limit of jobs that must never stop for good, such as the SDK's own maintenance: their failures keep
# retrying with the capped backoff and stay visible in the logs.
UNLIMITED = 2**31 - 1
DEFAULT_TIMEOUT = 300.0
DEFAULT_MAX_BACKLOG = 24
BACKOFF_START = 1.0
BACKOFF_CAP = 300.0


def seconds(v: float | timedelta) -> float:
    return v.total_seconds() if isinstance(v, timedelta) else float(v)


@dataclass
class Item:
    """A work item as a handler sees it."""

    id: str
    job: str
    kind: str
    attempts: int
    max_attempts: int
    operation_id: str
    args: Any = None  # decoded JSON
    progress: Any = None  # decoded JSON, singletons
    slot: str = ""  # cron
    call_results: dict[str, str] = field(default_factory=dict)


@dataclass
class Job:
    """One job a module declares. Durations are seconds (or timedelta)."""

    name: str
    kind: str
    handle: Callable[[Run | None], Awaitable[None]]  # every jobs get None
    effect: str = ""  # required except for every jobs
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    # cron jobs: the 5-field schedule, its IANA time zone, the backfill policy and its limit (P7.15, P7.16)
    cron: str = ""
    time_zone: str = ""
    backfill: str = LATEST
    max_backlog: int = DEFAULT_MAX_BACKLOG
    # singleton jobs run again this long after a run completes; every jobs run this often on every replica
    interval: float | timedelta = 0.0
    # one run's deadline: outbound calls need one (P4.4); a run past it fails as retryable
    timeout: float | timedelta = DEFAULT_TIMEOUT
    # bounds a business wait before it counts as a permanent failure; 0: no limit
    max_wait: float | timedelta = 0.0
    # runs in the transaction that marks the item dead: local writes and the outbox only
    dead_letter: Callable[[Tx, Item, BaseException], Awaitable[None]] | None = None
    schedule: Any = field(default=None, repr=False)  # the parsed cron schedule (besdk.work.cron)

    def prepare(self) -> None:
        """Checks the declaration and fills the defaults; raises ValueError."""
        if not self.name or self.handle is None:
            raise ValueError("work: a job needs a name and a handler")
        if self.kind not in KINDS:
            raise ValueError(f"work: job {self.name}: kind {self.kind!r}")
        if self.max_attempts <= 0:
            self.max_attempts = DEFAULT_MAX_ATTEMPTS
        self.interval, self.timeout, self.max_wait = seconds(self.interval), seconds(self.timeout), seconds(self.max_wait)
        if self.timeout <= 0:
            self.timeout = DEFAULT_TIMEOUT
        if self.kind != EVERY and self.effect not in EFFECTS:
            raise ValueError(f"work: job {self.name} must declare its effect class (one of {', '.join(EFFECTS)})")
        if self.kind == CRON:
            from besdk.work import cron

            if not self.time_zone:
                raise ValueError(f"work: cron job {self.name} must declare its time zone")
            if self.backfill not in (LATEST, ALL, NONE):
                raise ValueError(f"work: job {self.name}: backfill {self.backfill!r}")
            if self.max_backlog <= 0:
                self.max_backlog = DEFAULT_MAX_BACKLOG
            try:
                self.schedule = cron.parse(self.cron, self.time_zone)
            except ValueError as e:
                raise ValueError(f"work: job {self.name}: {e}") from None
        if self.kind in (SINGLETON, EVERY) and self.interval <= 0:
            raise ValueError(f"work: job {self.name} needs an interval")


class Jobs:
    """Collects a module's jobs (the module's jobs(j) method)."""

    def __init__(self) -> None:
        self._list: list[Job] = []

    def add(self, job: Job) -> None:
        self._list.append(job)

    def list(self) -> list[Job]:
        return list(self._list)


class Failure(Exception):
    """A handler's failure of a declared kind."""

    kind = "retryable"

    def __init__(self, cause: BaseException | str):
        super().__init__(str(cause))
        self.cause = cause


class Permanent(Failure):
    """Retrying cannot fix it: the item goes to dead."""

    kind = "permanent"


class NotEffective(Failure):
    """A failed non-idempotent call the handler confirmed did not take effect (refused before sending, or rejected by
    the other side): it may be retried."""

    kind = "not_effective"


class Wait(Failure):
    """A business wait (a source cannot be proven yet, the other side says "processing"): retried later without
    spending an attempt, up to the job's max_wait."""

    kind = "wait"


def kind_of(err: BaseException) -> str:
    return err.kind if isinstance(err, Failure) else "retryable"


class Lost(Exception):
    """This worker no longer holds the item: it was taken over, or the database epoch changed. The handler stops."""


def backoff(attempt: int) -> float:
    """The delay after attempt n: 1 s doubling, at most 5 minutes (P7.13)."""
    n = max(attempt, 1)
    return BACKOFF_CAP if n > 20 else min(BACKOFF_CAP, BACKOFF_START * 2 ** (n - 1))


def decode(v: Any) -> Any:
    """A jsonb column as asyncpg returns it (text without a codec)."""
    return json.loads(v) if isinstance(v, (str, bytes)) else v
