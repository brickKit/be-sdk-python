"""The NATS JetStream adapter (P6.16-P6.21): one file-stored stream per event stream, durable pull consumers, the
message ID for the broker's deduplication window, core NATS for best-effort notifications. The same streams and
consumers as the Go SDK's adapter, so components in either language share them."""
from __future__ import annotations

import asyncio
from typing import Callable

import nats
from nats.errors import MaxPayloadError, TimeoutError as NatsTimeout
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

from besdk.events.core import (TRANSPORT_RETENTION, Delivery, Handler, Message, Outcome, Stream, SubscriptionSpec,
                               TooLarge, jetstream_name, subject_matches)

DUPLICATE_WINDOW = 120.0  # the broker deduplicates a publish repeated within it; consumers dedupe the rest (P6.10)


def _redact(url: str) -> str:
    at, scheme = url.find("@"), url.find("://")
    return url[:scheme + 3] + "***" + url[at:] if 0 <= scheme < at else url


class _Subscription:
    def __init__(self) -> None:
        self.tasks: list[asyncio.Task] = []
        self.subs: list = []

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for s in self.subs:
            try:
                await s.unsubscribe()
            except Exception:  # noqa: BLE001 - already closed
                pass


class Transport:
    def __init__(self, nc: nats.NATS):
        self.nc, self.js = nc, nc.jetstream()

    @staticmethod
    async def connect(url: str, name: str) -> Transport:
        try:
            nc = await nats.connect(url, name=name, max_reconnect_attempts=-1)
        except Exception as e:  # noqa: BLE001
            raise ConnectionError(f"bus {_redact(url)}: {e}") from e
        return Transport(nc)

    @property
    def max_message(self) -> int:
        return self.nc.max_payload

    async def ensure_stream(self, s: Stream) -> None:
        """Created when missing; an existing stream is left as it is (P6.17)."""
        name = jetstream_name(s.name)
        try:
            await self.js.stream_info(name)
            return
        except NotFoundError:
            pass
        try:
            await self.js.add_stream(StreamConfig(name=name, subjects=[f"be.{s.name}.>"], storage=StorageType.FILE,
                                                  duplicate_window=DUPLICATE_WINDOW, max_age=TRANSPORT_RETENTION))
        except Exception:  # noqa: BLE001 - created concurrently by another replica
            await self.js.stream_info(name)

    async def publish(self, m: Message) -> None:
        """Returns after the stream's acknowledgement."""
        headers = dict(m.headers)
        if m.id:
            headers["Nats-Msg-Id"] = m.id
        size = len(m.data) + len(m.subject) + sum(len(k) + len(v) + 4 for k, v in headers.items())
        if size > self.max_message:
            raise TooLarge()
        try:
            await self.js.publish(m.subject, m.data, headers=headers)
        except MaxPayloadError as e:
            raise TooLarge() from e

    async def subscribe(self, spec: SubscriptionSpec, h: Handler) -> _Subscription:
        """One durable pull consumer per event stream, created when missing and left as it is when it exists."""
        spec = spec.defaults()
        sub = _Subscription()
        try:
            for stream in spec.streams:
                await self._consumer(stream, spec)
                psub = await self.js.pull_subscribe_bind(durable=spec.durable, stream=jetstream_name(stream))
                sub.subs.append(psub)
                sub.tasks.append(asyncio.create_task(self._pull(psub, spec, h)))
        except BaseException:
            await sub.stop()
            raise
        return sub

    async def _consumer(self, stream: str, spec: SubscriptionSpec) -> None:
        name = jetstream_name(stream)
        try:
            await self.js.consumer_info(name, spec.durable)
            return
        except NotFoundError:
            pass
        filters = [f for f in spec.filters if subject_matches(f"be.{stream}.>", f) or f.startswith(f"be.{stream}.")]
        cfg = ConsumerConfig(durable_name=spec.durable, ack_policy=AckPolicy.EXPLICIT, ack_wait=spec.ack_wait,
                             max_deliver=spec.max_deliver, deliver_policy=DeliverPolicy.ALL, max_ack_pending=1000)
        if len(filters) == 1:
            cfg.filter_subject = filters[0]
        elif filters:
            cfg.filter_subjects = filters
        elif spec.filters:
            cfg.filter_subject = f"be.{stream}.__none__"  # none of the filters belongs to this stream
        try:
            await self.js.add_consumer(name, cfg)
        except Exception:  # noqa: BLE001 - created concurrently by another instance; its settings stay
            await self.js.consumer_info(name, spec.durable)

    async def _pull(self, psub, spec: SubscriptionSpec, h: Handler) -> None:
        slots = asyncio.Semaphore(spec.concurrency)
        running: set[asyncio.Task] = set()
        try:
            while True:
                await slots.acquire()
                try:
                    msgs = await psub.fetch(1, timeout=1)
                except NatsTimeout:
                    slots.release()
                    continue
                except Exception:  # noqa: BLE001 - reconnecting; try again
                    slots.release()
                    await asyncio.sleep(0.2)
                    continue
                for msg in msgs:
                    t = asyncio.create_task(self._handle(spec, msg, h, slots))
                    running.add(t)
                    t.add_done_callback(running.discard)
        finally:
            for t in running:
                t.cancel()
            await asyncio.gather(*running, return_exceptions=True)

    async def _handle(self, spec: SubscriptionSpec, msg, h: Handler, slots: asyncio.Semaphore) -> None:
        try:
            attempt = msg.metadata.num_delivered if msg.metadata else 1
            headers = {k: v for k, v in (msg.headers or {}).items() if not k.startswith("Nats-")}
            d = Delivery(msg.subject, headers, msg.data, attempt, msg.in_progress)
            out = await h(d)
            if out == Outcome.ACK:
                await msg.ack_sync()
            elif out == Outcome.NAK:
                await msg.nak(delay=spec.backoff_for(attempt))
            else:
                await msg.term()
        except Exception:  # noqa: BLE001 - unacknowledged: redelivered after the ack wait
            pass
        finally:
            slots.release()

    async def notify(self, subject: str, data: bytes) -> None:
        """Core NATS: nothing is stored (P6.22)."""
        await self.nc.publish(subject, data)

    async def listen(self, subject: str, fn: Callable[[bytes], None]):
        async def cb(m) -> None:
            fn(m.data)

        s = await self.nc.subscribe(subject, cb=cb)
        await self.nc.flush()

        class _Listener:
            async def stop(self) -> None:
                await s.unsubscribe()

        return _Listener()

    async def delete_stream(self, stream: str) -> None:
        """Tests and decommissioning only."""
        await self.js.delete_stream(jetstream_name(stream))

    async def close(self) -> None:
        await self.nc.close()
