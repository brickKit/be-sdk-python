"""The clock that keeps counting while the system is suspended (protocol P1.11; security 03 counts validity periods
on it). On Linux that is CLOCK_BOOTTIME."""
import time


class ClockError(Exception):
    pass


def since_boot() -> float:
    """Seconds since boot including suspension."""
    return time.clock_gettime(time.CLOCK_BOOTTIME)


def check() -> None:
    """Raise when the clock is not available or goes backwards; the launcher then refuses to start (exit 69)."""
    try:
        a, b = since_boot(), since_boot()
    except (AttributeError, OSError) as e:
        raise ClockError(f"CLOCK_BOOTTIME is not available: {e}") from e
    if b < a:
        raise ClockError("CLOCK_BOOTTIME went backwards")
