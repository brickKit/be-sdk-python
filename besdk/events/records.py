"""The record reads of a member's streams (P9.30-P9.33): be.source.v1.RecordService.Read answers every asked ID in
order (exists, tombstone or purged), Snapshot pages the existing records by ID ascending, each page in its own read
snapshot with the partitions' high points; later pages are bound to the source's implementation instance and epoch.
Every answer declares who answered (P9.18). A record's content is its current full-state event's payload, the same
bytes; a big one is a claim check (P6.14)."""
from __future__ import annotations

import base64
import json

from besdk import errs
from besdk._gen.be.source.v1 import records_pb2, records_pb2_grpc, watermark_pb2
from besdk.db.pool import DB, Tx, TxOptions
from besdk.events.core import MAX_PAYLOAD, Stream
from besdk.events.objectstore import sha256_hex
from besdk.events.outbox import Producer
from besdk.events.sources import EXISTS, PURGED, RECORD_PAGE, TOMBSTONE
from besdk.obs import Logger
from besdk.system import Method, caller

SNAPSHOT = TxOptions(snapshot=True)  # REPEATABLE READ: the high points describe exactly what was read
STATES = {EXISTS: records_pb2.READ_STATE_EXISTS, TOMBSTONE: records_pb2.READ_STATE_TOMBSTONE,
          PURGED: records_pb2.READ_STATE_PURGED}


def method(producer: Producer) -> Method | None:
    """The service's system-surface declaration: the streams' operation and every stream's readers; None when the
    member declares no reads. Streams declaring different operations are refused."""
    op, callers = "", []
    for s in producer.streams.values():
        if s.records is None:
            continue
        if op and op != s.records.operation:
            raise ValueError(f"events: streams declare different record read operations ({op}, {s.records.operation})")
        op = s.records.operation
        callers += [c for c in s.records.readers if c not in callers]
    return Method(op, callers) if op else None


class RecordSource(records_pb2_grpc.RecordServiceServicer):
    def __init__(self, db: DB, producer: Producer, impl: str, deployment: str, log: Logger):
        self.db, self.producer, self.impl, self.deployment, self.log = db, producer, impl, deployment, log

    def _stream(self, name: str, method_: str) -> Stream:
        who = caller()
        s = self.producer.streams.get(name)
        if s is None or s.records is None:
            raise errs.new("NOT_FOUND", f"no stream {name!r} with record reads")
        if who not in s.records.readers:
            raise errs.new("PERMISSION_DENIED", f"{who} may not read {s.name}")
        self.log.info("record read", event="record_read", caller=who, stream=s.name, method=method_)
        return s

    async def _declare(self, tx: Tx, s: Stream) -> tuple[watermark_pb2.Declaration, tuple[int, int], list]:
        """What one read transaction declares: who answered, the epoch and the partitions' high points (P9.18)."""
        r = await tx.fetchrow("SELECT NOT pg_is_in_recovery() AS primary_, "
                              "(SELECT system_identifier::text FROM pg_control_system()) AS system")
        e = await tx.fetchrow("SELECT generation, timeline FROM be_current_db_epoch()")
        epoch = (e["generation"], e["timeline"])
        decl = watermark_pb2.Declaration(deployment=self.deployment, impl=self.impl, system_id=r["system"],
                                         epoch=f"{epoch[0]}.{epoch[1]}", primary=r["primary_"])
        highs = await tx.fetch("""SELECT g AS partition, coalesce((SELECT last_seq FROM be_outbox_partition p
            WHERE p.stream = $1 AND p.partition = g AND p.epoch_generation = $3 AND p.epoch_timeline = $4), 0) AS seq
            FROM generate_series(0, $2 - 1) g ORDER BY g""", s.name, s.partitions, epoch[0], epoch[1])
        return decl, epoch, [records_pb2.PartitionHigh(partition=h["partition"], seq=h["seq"]) for h in highs]

    async def _payload(self, s: Stream, rid: str, version: int, data) -> tuple[bytes, bool]:
        """A record's data encoded as the producer encodes an event's; a big one goes to object storage."""
        b = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode()
        if len(b) <= MAX_PAYLOAD:
            return b, False
        if self.producer.claims is None:
            raise RuntimeError(f"record {rid} of {s.name}: {len(b)} bytes and no object storage for a claim check")
        uri = await self.producer.claims.put(f"record-{s.name}-{rid}-{version}-{sha256_hex(b)[:16]}", b)
        return json.dumps({"claim": {"uri": uri, "size": len(b), "sha256": sha256_hex(b)}},
                          separators=(",", ":")).encode(), True

    async def Read(self, request: records_pb2.ReadRequest, context) -> records_pb2.ReadResponse:  # noqa: N802
        s = self._stream(request.stream, "Read")
        ids = list(request.record_ids)
        if not 1 <= len(ids) <= RECORD_PAGE:
            raise errs.new("REQUEST_INVALID", f"a read asks for 1-{RECORD_PAGE} record IDs")

        async def body(tx: Tx):
            decl, _, highs = await self._declare(tx, s)
            return decl, highs, await s.records.read(tx, ids)

        decl, highs, results = await self.db.background_tx(body, SNAPSHOT)
        if len(results) != len(ids):
            raise RuntimeError(f"stream {s.name}: the read answered {len(results)} of {len(ids)} IDs")
        resp = records_pb2.ReadResponse(source=decl, highs=highs)
        for asked, res in zip(ids, results):
            if res.id != asked:
                raise RuntimeError(f"stream {s.name}: the read answered {res.id} where {asked} was asked")
            if res.state not in STATES:
                raise RuntimeError(f"stream {s.name}: record {res.id} read as {res.state!r}")
            out = resp.results.add(record_id=res.id, state=STATES[res.state],
                                   version=0 if res.state == PURGED else res.version)
            if res.state == EXISTS:
                out.payload, out.claim = await self._payload(s, res.id, res.version, res.data)
        return resp

    async def Snapshot(self, request: records_pb2.SnapshotRequest, context) -> records_pb2.SnapshotResponse:  # noqa: N802
        tok = None
        name = request.stream
        if request.page_token:
            try:
                tok = json.loads(base64.urlsafe_b64decode(request.page_token + "=" * (-len(request.page_token) % 4)))
                name = tok["s"]
            except (ValueError, TypeError, KeyError):
                raise errs.new("REQUEST_INVALID", "page token") from None
        s = self._stream(name, "Snapshot")

        async def body(tx: Tx):
            decl, epoch, highs = await self._declare(tx, s)
            if tok is not None and (tok["i"] != self.impl or tuple(tok["e"]) != epoch):
                raise errs.new("QUERY_PLAN_CHANGED", "the source's implementation instance or epoch changed: align again")
            return decl, epoch, highs, await s.records.page(tx, tok["a"] if tok else "", RECORD_PAGE)

        decl, epoch, highs, records = await self.db.background_tx(body, SNAPSHOT)
        resp = records_pb2.SnapshotResponse(source=decl, highs=highs)
        for r in records[:RECORD_PAGE]:
            out = resp.records.add(record_id=r.id, version=r.version)
            out.payload, out.claim = await self._payload(s, r.id, r.version, r.data)
        if len(records) >= RECORD_PAGE:
            nxt = {"s": s.name, "a": records[RECORD_PAGE - 1].id, "i": self.impl, "e": list(epoch)}
            resp.next_page_token = base64.urlsafe_b64encode(json.dumps(nxt, separators=(",", ":")).encode()) \
                .decode().rstrip("=")
        return resp
