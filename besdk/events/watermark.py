"""The watermark source (P9.13-P9.18; the assembly project's protocol 02, steps 1-3): be.source.v1.WatermarkService.Pull
for a member's watermark streams. It checks the row at the consumer's contiguous position of each partition (vector
watermark-pull-check), writes an echo into the same outbox, numbers up to it with the forwarder's own code, and returns
the rows after the position up to the echo as the stream carries them, a page of 500 a partition at most; later pages
are bound to the source's implementation instance and epoch. Every page declares who answered (P9.18): a pull is
answered by the primary."""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass

from besdk import errs
from besdk._gen.be.source.v1 import watermark_pb2, watermark_pb2_grpc
from besdk.db.pool import DB, Tx
from besdk.events.core import Event, Projection, Stream
from besdk.events.outbox import ROW_COLUMNS, Producer, message_of, number, rows_of
from besdk.obs import Logger
from besdk.system import Method, caller

ECHO_TYPE = "be.source.echo.v1"
PULL_PAGE = 500
NEED_BEYOND_RETENTION, NEED_ROLLBACK_DETECTED = -1, -2  # kept in a token cursor's echo when after is -1
NUMBER_BATCH = 500


@dataclass(frozen=True)
class RowIdentity:
    """The row at a position as a pull names it: an echo's nonce, or a record's ID, version and row digest."""

    echo: str = ""
    record_id: str = ""
    version: int = 0
    digest: str = ""


def pull_check(impl: str, epoch: str, counter: int, pos: tuple[str, str, int] | None, want: RowIdentity,
               at: RowIdentity | None) -> str:
    """P9.15 for one partition: echo_only, ok, beyond_retention or rollback_detected. pos is the consumer's position
    (impl, epoch, seq), None for echo only; counter the partition's last sequence in the current epoch (0 when nothing
    is numbered in it yet); at the outbox row at the position's sequence (None: not there)."""
    if pos is None or pos[0] != impl or pos[1] != epoch:
        return "echo_only"
    if pos[2] > counter:
        return "rollback_detected"
    if at is None:
        return "beyond_retention"
    if at != want:
        return "rollback_detected"
    return "ok"


def method(producer: Producer) -> Method | None:
    """The pull's system-surface declaration for the member's watermark streams: their operation and every stream's
    pullers; None when the member has none. Streams declaring different operations are refused."""
    op, callers = "", []
    for s in producer.streams.values():
        if s.watermark is None:
            continue
        if op and op != s.watermark.operation:
            raise ValueError(f"events: watermark streams declare different pull operations ({op}, "
                             f"{s.watermark.operation})")
        op = s.watermark.operation
        callers += [c for c in s.watermark.pullers if c not in callers]
    return Method(op, callers) if op else None


def _epoch_str(e: tuple[int, int]) -> str:
    return f"{e[0]}.{e[1]}"


class WatermarkSource(watermark_pb2_grpc.WatermarkServiceServicer):
    def __init__(self, db: DB, producer: Producer, source: str, impl: str, deployment: str, log: Logger):
        self.db, self.producer, self.source, self.impl, self.deployment, self.log = (
            db, producer, source, impl, deployment, log)

    async def Pull(self, request: watermark_pb2.PullRequest, context) -> watermark_pb2.PullResponse:  # noqa: N802
        who = caller()
        tok: dict | None = None
        if request.page_token:
            try:
                tok = json.loads(base64.urlsafe_b64decode(request.page_token + "=" * (-len(request.page_token) % 4)))
            except (ValueError, TypeError):
                raise errs.new("REQUEST_INVALID", "page token") from None
            request = watermark_pb2.PullRequest(stream=tok["s"], nonce=tok["n"])
        s = self.producer.streams.get(request.stream)
        if s is None or s.watermark is None:
            raise errs.new("NOT_FOUND", f"no watermark stream {request.stream!r}")
        if who not in s.watermark.pullers:
            raise errs.new("PERMISSION_DENIED", f"{who} may not pull {s.name}")
        try:
            if len(bytes.fromhex(request.nonce)) < 16:
                raise ValueError
        except ValueError:
            raise errs.new("REQUEST_INVALID", "the nonce is at least 128 random bits, hexadecimal") from None
        primary, system = await self.db.background_tx(self._who_answers)
        if not primary:
            raise errs.new("NOT_READY", "a pull is answered by the primary")  # P9.18
        if tok is None:
            self.log.info("watermark pull", event="watermark_pull", caller=who, stream=s.name, nonce=request.nonce)
            if s.watermark.before is not None:
                try:
                    await s.watermark.before()
                except Exception as e:  # noqa: BLE001 - no echo: the watermark must not pass changes not brought in
                    self.log.warn("the source could not bring its changes up to date before the echo; no echo written",
                                  event="watermark_before_failed", stream=s.name, error=str(e))
                    raise errs.new("DEPENDENCY_UNAVAILABLE", "the source's changes could not be brought up to date") \
                        from e
            tok = await self._first_page(s, request, who)
        else:
            await self._same_source(tok)
        return await self._page(s, tok, system)

    @staticmethod
    async def _who_answers(tx: Tx) -> tuple[bool, str]:
        r = await tx.fetchrow("SELECT NOT pg_is_in_recovery() AS primary_, "
                              "(SELECT system_identifier::text FROM pg_control_system()) AS system")
        return r["primary_"], r["system"]

    async def _current_epoch(self) -> tuple[int, int]:
        r = await self.db.background_tx(lambda tx: tx.fetchrow("SELECT generation, timeline FROM be_current_db_epoch()"))
        return r["generation"], r["timeline"]

    async def _same_source(self, tok: dict) -> None:
        """A later page is refused once the instance or the epoch the first page declared changed (P9.16)."""
        if tok["i"] != self.impl or list(await self._current_epoch()) != tok["e"]:
            raise errs.new("QUERY_PLAN_CHANGED", "the source's implementation instance or epoch changed: pull again")

    async def _first_page(self, s: Stream, request: watermark_pb2.PullRequest, who: str) -> dict:
        """Checks every partition, writes the echoes, numbers up to them, and returns the cursors of all pages."""
        asked = {}
        for p in request.partitions:
            if p.partition >= s.partitions:
                raise errs.new("REQUEST_INVALID", f"partition {p.partition} of {s.name}")
            asked[p.partition] = p
        results: dict[int, tuple[str, int]] = {}

        async def body(tx: Tx) -> tuple[int, int]:
            results.clear()
            r = await tx.fetchrow("SELECT generation, timeline FROM be_current_db_epoch()")
            checked = (r["generation"], r["timeline"])
            for n in range(s.partitions):
                res = await self._check(tx, s.name, n, checked, asked.get(n))
                results[n] = res
                if res[0] in ("ok", "echo_only"):
                    await self.producer.insert(tx, s.name, n, Event(ECHO_TYPE, request.nonce, 1, Projection.FULL,
                                                                    {"nonce": request.nonce, "consumer": who}, subjects=[]))
            return checked

        checked = await self.db.background_tx(body)
        tok = {"s": s.name, "n": request.nonce, "i": self.impl, "e": list(checked), "p": []}
        for n in range(s.partitions):
            res, after = results[n]
            if res not in ("ok", "echo_only"):
                code = NEED_BEYOND_RETENTION if res == "beyond_retention" else NEED_ROLLBACK_DETECTED
                tok["p"].append({"p": n, "a": -1, "e": code})
                if res == "rollback_detected":
                    pos = asked[n].position
                    self.log.error("a pull found the row at the consumer's position replaced",
                                   event="watermark_rollback_detected", stream=s.name, partition=n,
                                   position=f"{pos.impl}/{pos.epoch}/{pos.seq}")
                continue
            echo, epoch = await self._number_echo(s.name, n, request.nonce)
            tok["e"] = list(epoch)
            if res == "echo_only" or epoch != checked:
                after = echo - 1  # the epoch moved on while numbering: the position belongs to another one, echo only
            tok["p"].append({"p": n, "a": after, "e": echo})
        return tok

    async def _check(self, tx: Tx, stream: str, n: int, current: tuple[int, int], asked) -> tuple[str, int]:
        """What P9.15 decides on, and the decision with the position's sequence."""
        if asked is None or not asked.HasField("position") or asked.position.impl != self.impl or \
                asked.position.epoch != _epoch_str(current):
            return "echo_only", 0
        pos = asked.position
        part = await tx.fetchrow("SELECT last_seq, epoch_generation, epoch_timeline FROM be_outbox_partition "
                                 "WHERE stream = $1 AND partition = $2", stream, n)
        counter = 0
        if part is not None and (part["epoch_generation"], part["epoch_timeline"]) == current:
            counter = part["last_seq"]  # else nothing numbered in this epoch yet
        row = await tx.fetchrow("""SELECT type, record_id, version, digest FROM be_outbox WHERE stream = $1
            AND partition = $2 AND epoch_generation = $3 AND epoch_timeline = $4 AND seq = $5""", stream, n,
                                current[0], current[1], pos.seq)
        at = None
        if row is not None:
            at = RowIdentity(echo=row["record_id"]) if row["type"] == ECHO_TYPE else \
                RowIdentity(record_id=row["record_id"], version=row["version"], digest=row["digest"])
        want = RowIdentity(echo=asked.row.echo_nonce)
        if asked.row.HasField("record"):
            rec = asked.row.record
            want = RowIdentity(record_id=rec.record_id, version=rec.version, digest=rec.digest)
        return pull_check(self.impl, _epoch_str(current), counter, (pos.impl, pos.epoch, pos.seq), want, at), pos.seq

    async def _number_echo(self, stream: str, n: int, nonce: str) -> tuple[int, tuple[int, int]]:
        """Numbers the partition until the echo has its position (the forwarder's numbering, P6.5)."""
        while True:
            r = await self.db.background_tx(lambda tx: tx.fetchrow(
                """SELECT seq, epoch_generation, epoch_timeline FROM be_outbox WHERE stream = $1 AND partition = $2
                AND type = $3 AND record_id = $4""", stream, n, ECHO_TYPE, nonce))
            if r is not None and r["seq"] is not None:
                return r["seq"], (r["epoch_generation"], r["epoch_timeline"])
            await number(self.db, stream, n, NUMBER_BATCH)

    async def _page(self, s: Stream, tok: dict, system: str) -> watermark_pb2.PullResponse:
        """Up to PULL_PAGE rows of every partition after its cursor and up to its echo."""
        epoch = tuple(tok["e"])
        resp = watermark_pb2.PullResponse(source=watermark_pb2.Declaration(
            deployment=self.deployment, impl=self.impl, system_id=system, epoch=_epoch_str(epoch), primary=True))
        nxt = {**tok, "p": []}
        for c in tok["p"]:
            res = resp.partitions.add(partition=c["p"])
            if c["a"] < 0:
                res.need_snapshot = (watermark_pb2.NEED_SNAPSHOT_BEYOND_RETENTION if c["e"] == NEED_BEYOND_RETENTION
                                     else watermark_pb2.NEED_SNAPSHOT_ROLLBACK_DETECTED)
                continue
            records = await self.db.background_tx(lambda tx, c=c: tx.fetch(
                ROW_COLUMNS + """ AND epoch_generation = $3 AND epoch_timeline = $4 AND seq > $5 AND seq <= $6
                ORDER BY seq LIMIT $7""", s.name, c["p"], epoch[0], epoch[1], c["a"], c["e"], PULL_PAGE + 1))
            rows = rows_of(records, s.name, c["p"])
            more = len(rows) > PULL_PAGE
            rows = rows[:PULL_PAGE]
            for r in rows:
                m, _ = await message_of(r, self.producer.claims, self.source, self.impl)
                res.events.add(headers=m.headers, payload=m.data)
            if more:
                nxt["p"].append({"p": c["p"], "a": rows[-1].seq, "e": c["e"]})
            elif not rows or rows[-1].seq != c["e"]:
                # the rows up to the echo are gone (purged meanwhile) or the epoch moved: this pull cannot finish
                raise errs.new("QUERY_PLAN_CHANGED", "the rows up to the echo are no longer there: pull again")
            else:
                res.echo.CopyFrom(watermark_pb2.Position(impl=self.impl, epoch=_epoch_str(epoch), seq=c["e"]))
        if nxt["p"]:
            resp.next_page_token = base64.urlsafe_b64encode(json.dumps(nxt, separators=(",", ":")).encode()) \
                .decode().rstrip("=")
        return resp
