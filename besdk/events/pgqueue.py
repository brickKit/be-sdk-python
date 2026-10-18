"""The PostgreSQL queue adapter (P6.16-P6.23, P6.27): messages, subscriptions and deliveries in the bus database's
own schema, read in (transaction ID, sequence) order below pg_snapshot_xmin. The same tables and channel as the Go
SDK's adapter, so components in either language share one queue."""
from __future__ import annotations

import asyncio
import base64
import json
import uuid
from typing import Callable
from urllib.parse import parse_qsl, urlsplit

import asyncpg

from besdk import protocol
from besdk.config import Secret
from besdk.db import Target
from besdk.db.migrate import MigrateConfig, MigrationSet, migrate
from besdk.events.core import (TRANSPORT_RETENTION, Delivery, Handler, Message, Outcome, Stream, SubscriptionSpec,
                               TooLarge, subject_matches)
from besdk.obs import Logger

MAX_MESSAGE = 1 << 20
DEFAULT_POLL = 0.1
ADVANCE_BATCH = 500
GIVE_UP_EVERY = 5.0  # spaces the deletion of exhausted deliveries whose lease ran out; claims skip them meanwhile
NOTIFY_CHANNEL = "be_bus_notify"
FIRST_PURGE_AFTER, PURGE_EVERY = 60.0, 3600.0


def parse(bus_url: str) -> tuple[dict, str]:
    """A pgqueue:// BUS_URL as connection arguments and the bus schema (default be_bus)."""
    u = urlsplit(bus_url)
    if u.scheme != "pgqueue":
        raise ValueError(f"bus: {_redact(bus_url)!r} is not a pgqueue:// URL")
    q = dict(parse_qsl(u.query))
    schema = q.pop("schema", "") or "be_bus"
    ssl = q.pop("sslmode", "prefer")
    args = {"host": u.hostname or "localhost", "port": u.port or 5432, "user": u.username, "password": u.password,
            "database": (u.path or "/").lstrip("/") or None, "ssl": None if ssl == "disable" else ssl}
    return args, schema


def _redact(s: str) -> str:
    u = urlsplit(s)
    if u.password:
        return u._replace(netloc=f"{u.username}@{u.hostname}" + (f":{u.port}" if u.port else "")).geturl()
    return s


class _Workers:
    def __init__(self, tasks: list[asyncio.Task]):
        self.tasks = tasks

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


class Transport:
    def __init__(self, pool: asyncpg.Pool, args: dict, schema: str):
        self.pool, self._args, self.schema, self.poll = pool, args, schema, DEFAULT_POLL
        self.on_error: Callable[[BaseException], None] | None = None
        self._purge = asyncio.create_task(self._purge_loop())

    @staticmethod
    async def connect(bus_url: str, name: str, max_conns: int = 8) -> Transport:
        args, schema = parse(bus_url)
        try:
            pool = await asyncpg.create_pool(**args, min_size=0, max_size=max(max_conns, 4), statement_cache_size=0,
                                             server_settings={"application_name": name + " bus", "timezone": "UTC"})
        except Exception as e:  # noqa: BLE001
            raise ConnectionError(f"bus {_redact(bus_url)}: {e}") from e
        return Transport(pool, args, schema)

    def t(self, name: str) -> str:
        return f'"{self.schema}"."{name}"'

    @property
    def max_message(self) -> int:
        return MAX_MESSAGE

    def _report(self, err: BaseException) -> None:
        if self.on_error is not None:
            self.on_error(err)

    async def ensure_stream(self, s: Stream) -> None:
        await self.pool.execute(f"INSERT INTO {self.t('be_bus_stream')} (name) VALUES ($1) ON CONFLICT DO NOTHING", s.name)

    async def publish(self, m: Message) -> None:
        """The insert's commit is the acknowledgement; a repeated message ID is a no-op."""
        if len(m.data) + len(m.subject) + sum(len(k) + len(v) for k, v in m.headers.items()) > MAX_MESSAGE:
            raise TooLarge()
        await self.pool.execute(f"""INSERT INTO {self.t('be_bus_message')} (msg_id, subject, headers, data)
            VALUES ($1, $2, $3::jsonb, $4) ON CONFLICT (msg_id) WHERE msg_id IS NOT NULL DO NOTHING""",
                                m.id or None, m.subject, json.dumps(m.headers), m.data)

    async def subscribe(self, spec: SubscriptionSpec, h: Handler) -> _Workers:
        """The durable is created with spec when missing; otherwise its stored settings apply."""
        spec = spec.defaults()
        await self.pool.execute(f"""INSERT INTO {self.t('be_bus_subscription')} (durable, streams, filters, max_deliver,
            ack_wait_ms, backoff_ms) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING""", spec.durable,
                                spec.streams, spec.filters, spec.max_deliver, int(spec.ack_wait * 1000),
                                [int(b * 1000) for b in spec.backoff])
        row = await self.pool.fetchrow(f"""SELECT streams, filters, max_deliver, ack_wait_ms, backoff_ms
            FROM {self.t('be_bus_subscription')} WHERE durable = $1""", spec.durable)
        stored = SubscriptionSpec(spec.durable, list(row["streams"]), list(row["filters"]), row["max_deliver"],
                                  row["ack_wait_ms"] / 1000, [b / 1000 for b in row["backoff_ms"]], spec.concurrency).defaults()
        return _Workers([asyncio.create_task(self._work(stored, h)) for _ in range(stored.concurrency)])

    @staticmethod
    def _matches(s: SubscriptionSpec, subj: str) -> bool:
        if not any(subject_matches(f"be.{st}.>", subj) for st in s.streams):
            return False
        return not s.filters or any(subject_matches(f, subj) for f in s.filters)

    async def _work(self, s: SubscriptionSpec, h: Handler) -> None:
        """Claims first and moves the read position only when nothing is due (P6.28): one advance queues a batch,
        which the next claims take, so a message costs its claim and its outcome; advancing before every claim cost
        a transaction of several round trips a message."""
        loop = asyncio.get_running_loop()
        gave_up = None
        while True:
            try:
                if gave_up is None or loop.time() - gave_up >= GIVE_UP_EVERY:
                    gave_up = loop.time()
                    await self._give_up(s)
                claimed = await self._claim(s)
                if claimed is None:
                    # the position moved: more may follow at once, so only an idle round waits (the queue is shared
                    # by every stream, and waiting after each batch read past capped a subscription at batch/poll)
                    if not await self._advance(s):
                        await asyncio.sleep(self.poll)
                    continue
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - reported, then tried again
                self._report(e)
                await asyncio.sleep(self.poll)
                continue
            d, token = claimed

            async def in_progress(token: str = token) -> None:
                await self.pool.execute(f"""UPDATE {self.t('be_bus_delivery')} SET lease_until = clock_timestamp()
                    + make_interval(secs => $3) WHERE durable = $1 AND lease_token = $2 AND in_flight""",
                                        s.durable, uuid.UUID(token), s.ack_wait)

            d.in_progress = in_progress
            try:
                out = await h(d)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - a handler's failure is a nak, never the end of the worker
                self._report(e)
                out = Outcome.NAK
            await asyncio.shield(self._settle(s, d, token, out))

    async def _advance(self, s: SubscriptionSpec) -> bool:
        """Move the read position over committed messages below pg_snapshot_xmin, queueing a delivery for every
        matching one (P6.23). True when the position moved."""
        async with self.pool.acquire() as conn, conn.transaction():
            pos = await conn.fetchrow(f"""SELECT pos_xid::text::bigint AS x, pos_seq AS q FROM {self.t('be_bus_subscription')}
                WHERE durable = $1 FOR UPDATE""", s.durable)
            rows = await conn.fetch(f"""SELECT xid::text::bigint AS x, seq, subject FROM {self.t('be_bus_message')}
                WHERE (xid, seq) > ($1::bigint::text::xid8, $2) AND xid < pg_snapshot_xmin(pg_current_snapshot())
                ORDER BY xid, seq LIMIT $3""", pos["x"], pos["q"], ADVANCE_BATCH)
            if not rows:
                return False
            seqs = [r["seq"] for r in rows if self._matches(s, r["subject"])]
            if seqs:
                await conn.execute(f"""INSERT INTO {self.t('be_bus_delivery')} (durable, seq)
                    SELECT $1, unnest($2::bigint[]) ON CONFLICT DO NOTHING""", s.durable, seqs)
            await conn.execute(f"""UPDATE {self.t('be_bus_subscription')} SET pos_xid = $2::bigint::text::xid8, pos_seq = $3
                WHERE durable = $1""", s.durable, rows[-1]["x"], rows[-1]["seq"])
            return True

    async def _give_up(self, s: SubscriptionSpec) -> None:
        """Deletes exhausted deliveries whose lease ran out (their last attempt was never settled). It looks only at
        in-flight rows (a partial index), so its cost does not grow with the backlog."""
        await self.pool.execute(f"""DELETE FROM {self.t('be_bus_delivery')} WHERE durable = $1 AND in_flight
            AND lease_until < clock_timestamp() AND attempts >= $2""", s.durable, s.max_deliver)

    async def _claim(self, s: SubscriptionSpec) -> tuple[Delivery, str] | None:
        """One due delivery: ready and past its backoff, or in flight past its lease with deliveries left (an
        exhausted one waits for _give_up). The cost does not grow with the backlog: a delivery whose message the
        retention purge removed (P6.27; the purge removes the deliveries with it, this covers one queued meanwhile)
        is dropped when it is claimed, instead of every claim checking the whole backlog for such rows first."""
        dlv, msg = self.t("be_bus_delivery"), self.t("be_bus_message")
        token = uuid.uuid7()
        # two paths, each along its own index in position order, and the claimed row located by its key (P6.28): a
        # condition joined with OR, or a join to the chosen row, let the planner read the whole backlog when the
        # statistics lagged behind it
        r = await self.pool.fetchrow(f"""WITH ready AS (SELECT seq FROM {dlv} WHERE durable = $1 AND NOT in_flight
                    AND not_before <= clock_timestamp() ORDER BY seq LIMIT 1 FOR UPDATE SKIP LOCKED),
                expired AS (SELECT seq FROM {dlv} WHERE durable = $1 AND in_flight AND lease_until < clock_timestamp()
                    AND attempts < $4 ORDER BY seq LIMIT 1 FOR UPDATE SKIP LOCKED),
                due AS (SELECT min(seq) AS seq FROM (SELECT seq FROM ready UNION ALL SELECT seq FROM expired) c),
            claimed AS (UPDATE {dlv} d SET in_flight = true, attempts = d.attempts + 1, lease_token = $2,
                lease_until = clock_timestamp() + make_interval(secs => $3)
                WHERE d.durable = $1 AND d.seq = (SELECT seq FROM due) RETURNING d.seq, d.attempts)
            SELECT c.seq, c.attempts, m.subject, m.headers, m.data FROM claimed c LEFT JOIN {msg} m ON m.seq = c.seq""",
                                     s.durable, token, s.ack_wait, s.max_deliver)
        if r is None:
            return None
        if r["subject"] is None:  # its message is gone: nothing to deliver
            await self.pool.execute(f"DELETE FROM {dlv} WHERE durable = $1 AND lease_token = $2 AND in_flight",
                                    s.durable, token)
            return None
        headers = json.loads(r["headers"]) if isinstance(r["headers"], str) else dict(r["headers"])
        return Delivery(r["subject"], headers, bytes(r["data"]), r["attempts"]), str(token)

    async def _settle(self, s: SubscriptionSpec, d: Delivery, token: str, out: Outcome) -> None:
        """Apply the handler's outcome, unless the lease was lost meanwhile (then the late outcome changes nothing)."""
        try:
            if out in (Outcome.ACK, Outcome.TERM) or (out == Outcome.NAK and d.attempt >= s.max_deliver):
                await self.pool.execute(f"DELETE FROM {self.t('be_bus_delivery')} WHERE durable = $1 AND lease_token = $2 "
                                        "AND in_flight", s.durable, uuid.UUID(token))
            else:
                await self.pool.execute(f"""UPDATE {self.t('be_bus_delivery')} SET in_flight = false, lease_token = NULL,
                    not_before = clock_timestamp() + make_interval(secs => $3) WHERE durable = $1 AND lease_token = $2
                    AND in_flight""", s.durable, uuid.UUID(token), s.backoff_for(d.attempt))
        except Exception as e:  # noqa: BLE001 - the lease runs out and the message is delivered again
            self._report(e)

    async def notify(self, subject: str, data: bytes) -> None:
        """NOTIFY: nothing is stored (P6.22)."""
        payload = json.dumps({"s": subject, "d": base64.b64encode(data).decode()}, separators=(",", ":"))
        await self.pool.execute("SELECT pg_notify($1, $2)", NOTIFY_CHANNEL, payload)

    async def listen(self, subject: str, fn: Callable[[bytes], None]):
        conn = await asyncpg.connect(**self._args)

        def on(_c, _pid, _channel, payload: str) -> None:
            try:
                msg = json.loads(payload)
                if msg.get("s") == subject:
                    fn(base64.b64decode(msg["d"]))
            except (ValueError, KeyError):
                pass

        await conn.add_listener(NOTIFY_CHANNEL, on)

        class _Listener:
            async def stop(self) -> None:
                await conn.close()

        return _Listener()

    async def purge(self, retention: float = TRANSPORT_RETENTION) -> int:
        """Messages older than retention whether or not every subscription read them, with their deliveries (P6.27)."""
        return await self.pool.fetchval(f"""WITH gone AS (DELETE FROM {self.t('be_bus_message')}
                WHERE published_at < now() - make_interval(secs => $1) RETURNING seq),
            deliveries AS (DELETE FROM {self.t('be_bus_delivery')} WHERE seq IN (SELECT seq FROM gone))
            SELECT count(*) FROM gone""", retention)

    async def _purge_loop(self) -> None:
        await asyncio.sleep(FIRST_PURGE_AFTER)
        while True:
            try:
                await self.purge()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self._report(e)
            await asyncio.sleep(PURGE_EVERY)

    async def close(self) -> None:
        self._purge.cancel()
        await asyncio.gather(self._purge, return_exceptions=True)
        await self.pool.close()


async def migrate_bus(owner_url: str, log: Logger) -> None:
    """Create or update the queue's tables in the bus schema, as the bus owner (provisioning, tests)."""
    args, schema = parse(owner_url)
    await migrate(MigrateConfig(Target(args["host"], args["port"], args["database"]), args["user"],
                                Secret(args["password"] or ""), schema, "bus", log),
                  MigrationSet("be-bus", "be_bus_migrations", protocol.ROOT.joinpath("migrations-bus")))
