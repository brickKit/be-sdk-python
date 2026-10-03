"""The bundle source (be-protocol P6.1): ``GET {AUTHZ_URL}/authz/v2/bundle`` with ``If-None-Match``,
every 15 s, at once on a poke, 3 s per fetch, first load retried from 0.5 s doubling to 15 s,
fail-static. Process-wide in a shell (P19.3)."""

from __future__ import annotations

import asyncio
import logging
import time

import httpx

from besdk.auth.bundle import Bundle, BundleRefused

POLL = 15.0
TIMEOUT = 3.0


class BundleSource:
    def __init__(self, authz_url: str, client: httpx.AsyncClient, logger: logging.Logger, *,
                 interval: float = POLL, first_backoff: float = 0.5, first_backoff_max: float = 15.0):
        self.url = authz_url.rstrip("/") + "/authz/v2/bundle"
        self.client, self.logger = client, logger
        self.interval = interval
        self.first_backoff, self.first_backoff_max = first_backoff, first_backoff_max
        self.bundle: Bundle | None = None
        self.loaded = asyncio.Event()
        self.loaded_at = 0.0
        self._etag = ""
        self._poke = asyncio.Event()

    def poke(self) -> None:
        """``infra.authz.changed.v1`` arrived: fetch now (P12.10)."""
        self._poke.set()

    def age(self) -> float:
        return time.monotonic() - self.loaded_at if self.loaded_at else 0.0

    async def fetch(self) -> bool:
        """One fetch; True when a bundle is held afterwards. Never raises for a provider failure."""
        headers = {"If-None-Match": self._etag} if self._etag else {}
        try:
            r = await self.client.get(self.url, headers=headers, timeout=TIMEOUT)
            if r.status_code == 304 and self.bundle is not None:
                self.loaded_at = time.monotonic()
                return True
            r.raise_for_status()
            doc = r.json()
        except (httpx.HTTPError, ValueError) as e:
            self.logger.warning("authz_bundle_fetch_failed", extra={"error": f"{type(e).__name__}: {e}"})
            return self.bundle is not None
        try:
            b = Bundle.accept(doc)
        except BundleRefused as e:
            self.logger.error("authz_bundle_refused", extra={"error": str(e)})
            return self.bundle is not None
        self.bundle, self._etag, self.loaded_at = b, r.headers.get("ETag", ""), time.monotonic()
        self.loaded.set()
        return True

    async def run(self) -> None:
        delay = self.first_backoff
        while not await self.fetch():
            await self._wait(delay)
            delay = min(delay * 2, self.first_backoff_max)
        while True:
            await self._wait(self.interval)
            await self.fetch()

    async def _wait(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._poke.wait(), seconds)
        except TimeoutError:
            pass
        self._poke.clear()
