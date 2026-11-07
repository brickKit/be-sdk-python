"""The producer's redelivery (P6.38): be.source.v1.ResendService for a member's streams sends the published rows of
each partition after the asked position again to the transport, in position order, with the event's identity and
headers and a message identity of its own (<event ID>/resend/<request ID>: when only the consumer's database went back,
the original message may still be in the broker's deduplication window). It answers how many it sent and the
partitions it cannot cover: beyond_retention (rows after the start were purged) and ahead (the start is past what the
partition has: the producer went back past what the consumer saw)."""
from __future__ import annotations

import uuid

from besdk import errs
from besdk._gen.be.source.v1 import resend_pb2, resend_pb2_grpc
from besdk.db.pool import DB, Tx
from besdk.events.core import Transport
from besdk.events.outbox import ROW_COLUMNS, Producer, message_of, rows_of
from besdk.events.progress import Point
from besdk.obs import Logger

RESEND_CALL_TIMEOUT = 600.0  # one call may send a partition's whole retention again
BATCH = 500


def _epoch(s: str) -> tuple[int, int]:
    g, _, t = s.partition(".")
    if not (g.isdigit() and t.isdigit()):
        raise errs.new("REQUEST_INVALID", "an epoch is <generation>.<timeline>")
    return int(g), int(t)


class Resender(resend_pb2_grpc.ResendServiceServicer):
    def __init__(self, db: DB, producer: Producer, transport: Transport, source: str, impl: str, log: Logger,
                 batch: int = BATCH):
        self.db, self.producer, self.transport = db, producer, transport
        self.source, self.impl, self.log, self.batch = source, impl, log, batch

    async def Resend(self, request: resend_pb2.ResendRequest, context) -> resend_pb2.ResendResponse:  # noqa: N802
        st = self.producer.streams.get(request.stream)
        if st is None:
            raise errs.new("REQUEST_INVALID", f"no such stream: {request.stream}")
        rid = request.request_id or str(uuid.uuid7())
        start: dict[int, Point] = {}
        for f in getattr(request, "from"):
            if f.partition >= st.partitions:
                raise errs.new("REQUEST_INVALID", "no such partition")
            if not f.HasField("after") or f.after.impl != self.impl:
                continue  # another implementation instance's position: from the first row kept
            g, t = _epoch(f.after.epoch)
            start[f.partition] = Point(g, t, f.after.seq)
        out = resend_pb2.ResendResponse()
        for n in range(st.partitions):
            after = start.get(n)
            beyond, ahead = await self._judge(st.name, n, after)
            if beyond:
                out.beyond_retention.append(n)
            if ahead:
                out.ahead.append(n)  # nothing after it to send: the consumer saw what is not here
                continue
            try:
                out.resent += await self._partition(st.name, n, after, rid)
            except Exception as e:  # noqa: BLE001
                self.log.error("a redelivery stopped", event="event_resend_failed", stream=st.name, partition=n,
                               error=str(e))
                raise errs.new("DEPENDENCY_UNAVAILABLE",
                               "the redelivery stopped (the transport or the database): ask again") from None
        self.log.info("redelivered", event="event_resent", stream=st.name, request=rid, resent=out.resent,
                      beyond_retention=list(out.beyond_retention), ahead=list(out.ahead))
        return out

    async def _judge(self, stream: str, n: int, after: Point | None) -> tuple[bool, bool]:
        """Whether rows after the start were purged, and whether the start is past what this partition has (past the
        fork point of its epoch, past the counter of the current one, or an epoch it never had)."""
        async def body(tx: Tx) -> tuple[bool, bool]:
            r = await tx.fetchrow("""SELECT last_seq, epoch_generation, epoch_timeline, purged_generation,
                purged_timeline, purged_seq FROM be_outbox_partition WHERE stream = $1 AND partition = $2""", stream, n)
            if r is None:
                return False, after is not None  # nothing ever numbered here
            beyond = False
            if r["purged_seq"] is not None:
                purged = Point(r["purged_generation"], r["purged_timeline"], r["purged_seq"])
                beyond = after is None or after < purged
            if after is None:
                return beyond, False
            if r["epoch_generation"] is not None and (after.generation, after.timeline) == (r["epoch_generation"],
                                                                                           r["epoch_timeline"]):
                return beyond, after.seq > r["last_seq"]
            fork = await tx.fetchval("""SELECT fork_seq FROM be_epoch_history WHERE stream = $1 AND partition = $2
                AND old_generation = $3 AND old_timeline = $4""", stream, n, after.generation, after.timeline)
            if fork is None:
                return beyond, True  # an epoch this partition never had
            return beyond, after.seq > fork

        return await self.db.background_tx(body)

    async def _partition(self, stream: str, n: int, after: Point | None, rid: str) -> int:
        """One partition's published rows after the start, a batch at a time, in position order."""
        sent = 0
        while True:
            async def read(tx: Tx, after=after) -> list:
                if after is None:
                    return await tx.fetch(ROW_COLUMNS + """ AND published_at IS NOT NULL
                        ORDER BY epoch_generation, epoch_timeline, seq LIMIT $3""", stream, n, self.batch)
                return await tx.fetch(ROW_COLUMNS + """ AND published_at IS NOT NULL
                    AND (epoch_generation, epoch_timeline, seq) > ($3, $4, $5)
                    ORDER BY epoch_generation, epoch_timeline, seq LIMIT $6""", stream, n, after.generation,
                                      after.timeline, after.seq, self.batch)

            rows = rows_of(await self.db.background_tx(read), stream, n)
            for row in rows:
                m, _ = await message_of(row, self.producer.claims, self.source, self.impl)
                m.msg_id = f"{row.event_id}/resend/{rid}"
                await self.transport.publish(m)
                sent += 1
                after = Point(row.epoch[0], row.epoch[1], row.seq)
            if len(rows) < self.batch:
                return sent
