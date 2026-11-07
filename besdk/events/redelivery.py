"""A member's consumer side after its database went back (P6.39, P6.40): it notices from the database epoch, asks every
producer it has seen for a redelivery from the smallest progress of its subscriptions, and records what a redelivery
cannot cover. It receives as usual meanwhile (the inbox deduplicates). The read models' repair from a snapshot belongs
to the projection consumer, not in this SDK yet (A164): every subscription records its gaps."""
from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from typing import Any

from besdk import reqctx
from besdk._gen.be.source.v1 import resend_pb2, watermark_pb2
from besdk.calls.client import options
from besdk.db.pool import DB, Tx
from besdk.events.progress import DELIVERY_LIFETIME, Point, SourceKey, progress
from besdk.events.resend import RESEND_CALL_TIMEOUT
from besdk.obs import Logger

RESEND_METHOD = "/be.source.v1.ResendService/Resend"


def from_of(component: str, stream: str, subs: list[str],
            progress_by_sub: dict[str, dict[SourceKey, Point]]) -> list[resend_pb2.PartitionFrom]:
    """Where each partition's redelivery starts (P6.39): the smallest progress of the subscriptions that read the source
    stream, when every one of them has one there on the same implementation instance; the other partitions are left
    out, so they start at the first row kept."""
    starts: dict[int, tuple[str, Point]] = {}
    open_: set[int] = set()
    for i, s in enumerate(subs):
        mine: set[int] = set()
        for k, p in progress_by_sub.get(s, {}).items():
            if k.component != component or k.stream != stream:
                continue
            mine.add(k.partition)
            cur = starts.get(k.partition)
            if k.partition in open_:
                pass
            elif i > 0 and cur is None:
                pass  # an earlier subscription has none here: no start, so from the first row kept
            elif cur is None:
                starts[k.partition] = (k.impl, p)
            elif cur[0] != k.impl:
                open_.add(k.partition)
            elif p < cur[1]:
                starts[k.partition] = (cur[0], p)
        open_.update(n for n in starts if n not in mine)
    return [resend_pb2.PartitionFrom(partition=n, after=watermark_pb2.Position(
        impl=starts[n][0], epoch=f"{starts[n][1].generation}.{starts[n][1].timeline}", seq=starts[n][1].seq))
        for n in sorted(starts) if n not in open_]


class Redelivery:
    """conn(component) is the producing component's system surface (the dependency's channel), None when no address is
    configured."""

    def __init__(self, db: DB, conn: Callable[[str], Any], log: Logger, lifetime: float = DELIVERY_LIFETIME):
        self.db, self.conn, self.log, self.lifetime = db, conn, log, lifetime

    async def went_back(self) -> tuple[bool, tuple[int, int]]:
        """Compares the database's epoch with the one the consumer side last ran on; the first start records it."""
        async def body(tx: Tx) -> tuple[bool, tuple[int, int]]:
            cur = await tx.fetchrow("SELECT generation, timeline FROM be_current_db_epoch()")
            now = (cur["generation"], cur["timeline"])
            last = await tx.fetchrow("SELECT epoch_generation, epoch_timeline FROM be_consumer_epoch")
            if last is None:
                await tx.execute("""INSERT INTO be_consumer_epoch (epoch_generation, epoch_timeline) VALUES ($1, $2)
                    ON CONFLICT DO NOTHING""", *now)
                return False, now
            return (last["epoch_generation"], last["epoch_timeline"]) != now, now

        return await self.db.background_tx(body)

    async def settle(self, epoch: tuple[int, int]) -> None:
        """Records the epoch the consumer side now runs on, once every producer answered."""
        await self.db.background_tx(lambda tx: tx.execute(
            "UPDATE be_consumer_epoch SET epoch_generation = $1, epoch_timeline = $2, updated_at = now()", *epoch))

    async def request(self) -> None:
        """Asks each producer seen in the inbox for a redelivery of each stream and records the partitions it cannot
        cover; raises when some producer did not answer (ask again later)."""
        rows = await self.db.background_tx(lambda tx: tx.fetch(
            """SELECT DISTINCT subscription, source_component, source_stream FROM be_inbox
            WHERE source_component IS NOT NULL AND seq IS NOT NULL"""))
        seen: dict[tuple[str, str], list[str]] = {}
        for r in rows:
            seen.setdefault((r["source_component"], r["source_stream"]), []).append(r["subscription"])
        by_sub = {s: await progress(self.db, s, self.lifetime) for subs in seen.values() for s in set(subs)}
        failures = []
        for (component, stream), subs in sorted(seen.items()):
            try:
                await self._one(component, stream, sorted(subs), by_sub)
            except Exception as e:  # noqa: BLE001 - the others are asked anyway
                failures.append(f"{component} ({stream}): {e}")
        if failures:
            raise RuntimeError("; ".join(failures))

    async def _one(self, component: str, stream: str, subs: list[str], by_sub: dict) -> None:
        channel = self.conn(component)
        if channel is None:
            self.log.error("a producer cannot be asked for a redelivery: no address for it",
                           event="event_resend_unreachable", component=component, stream=stream)
            return  # the recovery tool asks for it (P6.39); asking again would not change it
        req = resend_pb2.ResendRequest(stream=stream, request_id=str(uuid.uuid7()))
        getattr(req, "from").extend(from_of(component, stream, subs, by_sub))
        call = channel.unary_unary(RESEND_METHOD, request_serializer=resend_pb2.ResendRequest.SerializeToString,
                                   response_deserializer=resend_pb2.ResendResponse.FromString)
        with reqctx.deadline_scope(time.monotonic() + RESEND_CALL_TIMEOUT):  # calls need a deadline (P4.4)
            resp = await call(req, metadata=options(idempotent=True))
        self.log.info("a producer redelivered", event="event_resend_requested", component=component, stream=stream,
                      resent=resp.resent, beyond_retention=list(resp.beyond_retention), ahead=list(resp.ahead))
        for kind, parts in (("beyond_retention", resp.beyond_retention), ("ahead", resp.ahead)):
            for n in parts:
                await self._gap(kind, component, stream, n, subs, by_sub)

    async def _gap(self, kind: str, component: str, stream: str, n: int, subs: list[str], by_sub: dict) -> None:
        """Records a partition a redelivery could not cover, for every subscription that reads it (P6.40)."""
        for s in subs:
            at = next((f"{k.impl}/{p}" for k, p in by_sub.get(s, {}).items()
                       if (k.component, k.stream, k.partition) == (component, stream, n)), None)
            await self.db.background_tx(lambda tx, s=s, at=at: tx.execute(
                """INSERT INTO be_resend_gap (subscription, source_component, source_stream, source_partition, kind,
                progress) VALUES ($1, $2, $3, $4, $5, $6)""", s, component, stream, n, kind, at))
            self.log.error("a redelivery cannot cover a subscription's progress",
                           event="event_resend_beyond_retention" if kind == "beyond_retention"
                           else "event_resend_consumer_ahead",
                           subscription=s, component=component, stream=stream, partition=n, progress=at)

    async def run(self) -> None:
        """At start: when the database went back, ask until every producer answered, then settle; else nothing."""
        while True:
            try:
                back, cur = await self.went_back()
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self.log.warn("the consumer's database epoch could not be read; trying again", error=str(e))
                await asyncio.sleep(5)
        if not back:
            return
        self.log.warn("the database went back since the consumer side last ran: asking the producers for a redelivery",
                      event="event_resend_needed", epoch=f"{cur[0]}.{cur[1]}")
        wait = 1.0
        while True:
            try:
                await self.request()
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self.log.warn("a redelivery was not answered; asking again", error=str(e), wait_s=wait)
                await asyncio.sleep(wait)
                wait = min(2 * wait, 60.0)
        try:
            await self.settle(cur)
        except Exception as e:  # noqa: BLE001
            self.log.error("the consumer's epoch could not be recorded: the next start asks again", error=str(e))
