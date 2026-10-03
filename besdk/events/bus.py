"""The bus connection (be-protocol P12.4, P12.12, P12.13): NATS JetStream through nats-py. One connection
per process (P19.3), reconnecting forever every 2 s; streams are created when missing and never changed."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import nats
from nats.js.api import DiscardPolicy, RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

from besdk.events.envelope import stream_filter, stream_of

DAY = 86400.0
DLQ_STREAM, DLQ_SUBJECTS = "BE_DLQ", "dlq.>"


def stream_config(name: str, subjects: list[str], max_age: float) -> StreamConfig:
    """P12.4: 7 days, 1 GiB, discard old, 10 min duplicate window, file storage, one replica."""
    return StreamConfig(name=name, subjects=subjects, max_age=max_age, max_bytes=1 << 30,
                        discard=DiscardPolicy.OLD, duplicate_window=600.0, storage=StorageType.FILE,
                        num_replicas=1, retention=RetentionPolicy.LIMITS)


class Bus:
    def __init__(self, url: str, name: str, logger: logging.Logger):
        if not url.startswith("nats://"):
            raise ValueError(f"bus adapter for {url.split(':', 1)[0]}:// is not available in this SDK version")
        self.url, self.name, self.logger = url, name, logger
        self.nc: Any = None
        self.js: Any = None
        self._streams: set[str] = set()
        self._lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        return self.nc is not None and self.nc.is_connected

    async def connect(self) -> None:
        """Connect once; the caller retries with backoff while the bus is not up (P1.2)."""
        if self.nc is not None:
            return
        log = self.logger

        async def disconnected():
            log.warning("bus_disconnected")

        async def reconnected():
            log.info("bus_reconnected")

        async def error(e):
            log.warning("bus_error", extra={"error": f"{type(e).__name__}: {e}"})

        self.nc = await nats.connect(servers=[self.url], name=self.name, max_reconnect_attempts=-1,
                                     reconnect_time_wait=2, allow_reconnect=True, connect_timeout=3,
                                     disconnected_cb=disconnected, reconnected_cb=reconnected, error_cb=error)
        self.js = self.nc.jetstream()

    async def ensure_stream_for(self, subject: str) -> str:
        return await self._ensure(stream_of(subject), [stream_filter(subject)], 7 * DAY)

    async def ensure_dlq(self) -> str:
        return await self._ensure(DLQ_STREAM, [DLQ_SUBJECTS], 30 * DAY)

    async def _ensure(self, name: str, subjects: list[str], max_age: float) -> str:
        async with self._lock:
            if name in self._streams:
                return name
            try:
                await self.js.stream_info(name)
            except NotFoundError:
                try:
                    await self.js.add_stream(stream_config(name, subjects, max_age))
                    self.logger.info("stream_created", extra={"stream": name})
                except Exception as e:  # noqa: BLE001 - another process may have created it meanwhile
                    try:
                        await self.js.stream_info(name)
                    except NotFoundError:
                        raise RuntimeError(f"stream {name} is missing and cannot be created: {e}") from e
            self._streams.add(name)
            return name

    async def publish(self, subject: str, data: bytes, headers: dict[str, str], timeout: float = 5.0) -> Any:
        """Publish and wait for the broker's PubAck (P12.1)."""
        return await self.js.publish(subject, data, timeout=timeout, headers=headers)

    async def close(self) -> None:
        if self.nc is not None:
            try:
                await self.nc.drain()
            except Exception:  # noqa: BLE001
                await self.nc.close()
            self.nc = None
