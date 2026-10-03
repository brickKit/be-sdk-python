"""Supervision of background work (be-protocol P1.7): every loop the runtime runs (bundle polling, the
outbox pump, consumers, secret polling, later jobs) is started here. A failure is recovered, logged and
counted, and the work restarts after an exponential backoff from 1 s to 5 min; nothing ends the process."""

from __future__ import annotations

import asyncio
import logging
import random
from collections import defaultdict
from typing import Awaitable, Callable


class Supervisor:
    def __init__(self, logger: logging.Logger, *, initial: float = 1.0, maximum: float = 300.0):
        self.logger = logger
        self.initial, self.maximum = initial, maximum
        self.failures: dict[str, int] = defaultdict(int)
        self._tasks: dict[str, asyncio.Task] = {}
        self.on_failure: Callable[[str], None] | None = None

    def start(self, name: str, work: Callable[[], Awaitable[None]]) -> asyncio.Task:
        task = asyncio.create_task(self._run(name, work), name=f"besdk:{name}")
        self._tasks[name] = task
        return task

    async def _run(self, name: str, work: Callable[[], Awaitable[None]]) -> None:
        delay = self.initial
        while True:
            try:
                await work()
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - supervision boundary
                self.failures[name] += 1
                if self.on_failure:
                    self.on_failure(name)
                self.logger.error("background_work_failed", extra={"work": name, "error": f"{type(e).__name__}: {e}",
                                                                   "retry_in_s": round(delay, 3)})
                await asyncio.sleep(delay * (0.8 + 0.4 * random.random()))
                delay = min(delay * 2, self.maximum)

    async def stop(self, timeout: float | None = None) -> None:
        """Cancel every piece of work and wait for it to finish its cleanup."""
        tasks = list(self._tasks.values())
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=timeout)
        self._tasks.clear()
