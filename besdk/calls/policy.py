"""The numbers of be-protocol chapter 04: per-attempt timeouts, the retry decision and backoff (P4.4-P4.7), the retry
budget (P4.8) and the circuit breaker (P4.11-P4.14). All times are seconds; the breaker runs on a monotonic clock."""
from __future__ import annotations

import threading

MAX_ATTEMPT_TIMEOUT = 3.0
DEADLINE_RESERVE = 0.05
MAX_ATTEMPTS = 3
BACKOFF_BASE = 0.1
BACKOFF_CAP = 1.0
OUTBOUND_LIMIT = 64
BREAKER_WINDOW = 20
BREAKER_OPEN_FIRST = 5.0  # P4.13: the first open period; a failed trial doubles it
BREAKER_OPEN_MAX = 30.0  # up to this; closing resets it to BREAKER_OPEN_FIRST
BREAKER_TRIALS_TO_CLOSE = 3

OUTCOME_UNAVAILABLE, OUTCOME_CONNECT_FAILED, OUTCOME_NONE = "UNAVAILABLE", "CONNECT_FAILED_BEFORE_SEND", "NONE"


def attempt_timeout(remaining: float | None) -> float | None:
    """P4.4: min(3 s, remaining - 50 ms); None (do not send) without a deadline or with less than 50 ms left."""
    if remaining is None or remaining < DEADLINE_RESERVE:
        return None
    return min(MAX_ATTEMPT_TIMEOUT, remaining - DEADLINE_RESERVE)


def should_retry(idempotent: bool, outcome: str, attempts_made: int, budget_allows: bool,
                 remaining_after_backoff: float | None) -> bool:
    """P4.6 and P4.7: only idempotent methods, only when the dependency itself is unavailable, at most three
    attempts, only while the budget allows and while another attempt would still be sent after the backoff."""
    if not idempotent or outcome not in (OUTCOME_UNAVAILABLE, OUTCOME_CONNECT_FAILED):
        return False
    if attempts_made >= MAX_ATTEMPTS or not budget_allows:
        return False
    return attempt_timeout(remaining_after_backoff) is not None


def max_backoff(failed: int) -> float:
    """The cap of the full-jitter backoff after failed failures: min(1 s, 100 ms * 2^(n-1))."""
    if failed < 1:
        return 0.0
    return min(BACKOFF_CAP, BACKOFF_BASE * (1 << min(failed - 1, 4)))


BUDGET_SUCCESS, BUDGET_UNAVAILABLE, BUDGET_OTHER = "success", "unavailable", "other"


class Budget:
    """The retry budget in thousandths: gRPC retryThrottling with maxTokens 10 and tokenRatio 0.1."""

    MAX, SUCCESS, FAILURE, THRESHOLD = 10000, 100, 1000, 5000

    def __init__(self) -> None:
        self._tokens, self._lock = self.MAX, threading.Lock()

    def record(self, outcome: str) -> None:
        with self._lock:
            if outcome == BUDGET_SUCCESS:
                self._tokens = min(self.MAX, self._tokens + self.SUCCESS)
            elif outcome == BUDGET_UNAVAILABLE:
                self._tokens = max(0, self._tokens - self.FAILURE)

    @property
    def tokens(self) -> int:
        return self._tokens

    def allows_retry(self) -> bool:
        return self._tokens > self.THRESHOLD


CLOSED, OPEN, HALF_OPEN = "closed", "open", "half_open"
BREAKER_SUCCESS, BREAKER_FAILURE, BREAKER_IGNORED = "success", "failure", "ignored"


class Breaker:
    """The circuit breaker of one (member, dependency). start admits a call (a permit) or rejects it (CIRCUIT_OPEN)."""

    def __init__(self) -> None:
        self.state = CLOSED
        self._window: list[bool] = []  # True: a dependency failure
        self._opened_at = 0.0
        self._open_for = BREAKER_OPEN_FIRST  # the current open period (P4.13)
        self._trial = 0  # the permit holding the half-open trial slot, 0 when free
        self._trial_ok = 0
        self._next = 0
        self._lock = threading.Lock()

    def start(self, now: float) -> int | None:
        with self._lock:
            self._next += 1
            permit = self._next
            if self.state == OPEN:
                if now - self._opened_at < self._open_for:
                    return None
                self.state, self._trial, self._trial_ok = HALF_OPEN, 0, 0
            if self.state == HALF_OPEN:
                if self._trial:
                    return None
                self._trial = permit
            return permit

    def end(self, permit: int, now: float, outcome: str) -> None:
        with self._lock:
            if self.state == HALF_OPEN:
                if self._trial != permit:
                    return  # a late result of a call admitted before the breaker opened
                self._trial = 0
                if outcome == BREAKER_FAILURE:
                    self.state, self._opened_at, self._trial_ok = OPEN, now, 0
                    self._open_for = min(2 * self._open_for, BREAKER_OPEN_MAX)
                elif outcome == BREAKER_SUCCESS:
                    self._trial_ok += 1
                    if self._trial_ok >= BREAKER_TRIALS_TO_CLOSE:
                        self.state, self._window, self._open_for = CLOSED, [], BREAKER_OPEN_FIRST
            elif self.state == CLOSED and outcome != BREAKER_IGNORED:
                self._window = (self._window + [outcome == BREAKER_FAILURE])[-BREAKER_WINDOW:]
                if len(self._window) == BREAKER_WINDOW and sum(self._window) * 2 > BREAKER_WINDOW:
                    self.state, self._opened_at, self._window = OPEN, now, []
