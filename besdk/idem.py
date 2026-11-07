"""Idempotency keys of commands (protocol P4.21-P4.25; vector idempotency): the claim, the command's writes and its
outcome commit in one transaction; the same key and request again returns the stored outcome without running the
command; the same key with another request is IDEMPOTENCY_KEY_REUSED; a first use not yet committed is
IDEMPOTENCY_IN_PROGRESS. Records live 24 hours in the component's be_idempotency; the SDK's hourly job removes the
expired ones, and an expired record not yet removed counts as gone."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import asyncpg

from besdk import canon, errs
from besdk.calls.client import HEADER_IDEMPOTENCY_KEY, METADATA_IDEMPOTENCY_KEY
from besdk.db.batch import delete_in_batches
from besdk.db.pool import DB, Tx, rows_affected

RETENTION = 24 * 3600.0  # P4.25; the assembly project's protocol 10
CLEANUP_EVERY = 3600.0


def valid_key(k: str) -> bool:
    """1-255 visible ASCII characters (P4.21)."""
    return 1 <= len(k) <= 255 and all(0x21 <= ord(ch) <= 0x7E for ch in k)


@dataclass(frozen=True)
class Request:
    """One command's idempotency identity: who sent it, the key, and what the request was."""

    principal: str  # the user's subject, or the calling component on the system surface
    key: str
    operation: str
    target: str  # "<METHOD> <path>[?<query>]" or the full gRPC method
    body: bytes  # the body's raw bytes; a gRPC message deterministically serialized

    def fingerprint(self) -> str:
        """The request's canonical digest (P4.22)."""
        return canon.digest("be.idempotency.request", 1, {
            "principal": canon.text(self.principal), "operation": canon.text(self.operation),
            "target": canon.text(self.target), "body_sha256": hashlib.sha256(self.body).hexdigest()})


def _key_error(key: str) -> errs.Error:
    reason = "must be 1-255 visible ASCII characters" if key else "is missing"
    return errs.new("REQUEST_INVALID", f"the Idempotency-Key header {reason}").with_violations(
        errs.Violation("Idempotency-Key", "INVALID", reason))


async def from_http(c: Any) -> Request:
    """A user-surface command's request: the verified subject, the Idempotency-Key header, the route's operation, the
    method and request URI, and the body."""
    from besdk.authz.subject import subject

    key = c.header(HEADER_IDEMPOTENCY_KEY)
    if not valid_key(key):
        raise _key_error(key)
    s = subject()
    if s is None:
        raise errs.new("INTERNAL").wrap(RuntimeError("idempotency keys need an authenticated route"))
    url = c.request.url
    target = f"{c.request.method} {url.path}" + (f"?{url.query}" if url.query else "")
    return Request(s.principal, key, c.operation, target, await c.body())


def from_grpc(context: Any, operation: str, message: Any) -> Request:
    """A system-surface command's request: the calling component, the idempotency-key metadata, the operation, the
    full method and the message serialized deterministically."""
    from besdk.system import caller, method as current_method

    md = dict(context.invocation_metadata() or ())
    key = md.get(METADATA_IDEMPOTENCY_KEY, "")
    if not valid_key(key):
        raise _key_error(key)
    who, method = caller(), current_method()
    if not who or not method:
        raise errs.new("INTERNAL").wrap(RuntimeError("idempotency keys need a system-surface call"))
    return Request(who, key, operation, method, message.SerializeToString(deterministic=True))


@dataclass
class Outcome:
    """What the command decided (P4.24): a status and a result such as the record's or process's ID. A replay returns
    it, and the handler answers by reading under the caller's current authorization."""

    status: int = 200
    result: Any = None  # JSON-encodable


async def do(db: DB, r: Request, fn: Callable[[Tx], Awaitable[Outcome]]) -> tuple[Outcome, bool]:
    """Runs fn once per key (P4.23): in one transaction it claims the key, runs fn and stores its outcome. Returns the
    outcome and whether it was replayed (fn not run). When fn fails, nothing is kept and the key can be used again."""
    fp = r.fingerprint()
    got: dict[str, Any] = {}

    async def body(tx: Tx) -> None:
        got.clear()
        try:
            status = await tx.execute("""INSERT INTO be_idempotency (principal, key, fingerprint, operation, status,
                expires_at) VALUES ($1, $2, $3, $4, 0, now() + make_interval(secs => $5))
                ON CONFLICT (principal, key) DO UPDATE SET fingerprint = excluded.fingerprint,
                    operation = excluded.operation, status = 0, result = NULL, created_at = now(),
                    expires_at = excluded.expires_at
                WHERE be_idempotency.expires_at <= now()""", r.principal, r.key, fp, r.operation, RETENTION)
        except asyncpg.LockNotAvailableError:
            raise errs.new("IDEMPOTENCY_IN_PROGRESS", "the first request with this key has not finished") from None
        if rows_affected(status) == 0:
            row = await tx.fetchrow("SELECT fingerprint, status, result FROM be_idempotency WHERE principal = $1 "
                                    "AND key = $2", r.principal, r.key)
            if row is None:  # the earlier claim was removed between the insert and this read (cleanup)
                raise errs.new("IDEMPOTENCY_IN_PROGRESS", "the key's earlier use is being removed; retry")
            if row["fingerprint"] != fp:
                raise errs.new("IDEMPOTENCY_KEY_REUSED", "this idempotency key was used for a different request")
            res = row["result"]
            got["out"], got["replayed"] = Outcome(row["status"], json.loads(res) if isinstance(res, str) else res), True
            return
        out = await fn(tx)
        if not out.status:
            out.status = 200
        await tx.execute("UPDATE be_idempotency SET status = $3, result = $4::jsonb WHERE principal = $1 AND key = $2",
                         r.principal, r.key, out.status, None if out.result is None else json.dumps(out.result))
        got["out"], got["replayed"] = out, False

    await db.tx(body)
    return got["out"], got["replayed"]


async def cleanup(db: DB) -> int:
    """Deletes expired records in batches and returns how many (P4.25)."""
    return await delete_in_batches(db, """DELETE FROM be_idempotency WHERE (principal, key) IN (
        SELECT principal, key FROM be_idempotency WHERE expires_at < now() LIMIT $1 FOR UPDATE SKIP LOCKED)""")


def cleanup_job(db: DB):
    """The hourly cleanup every member with a database runs (P4.25): a besdk.work singleton."""
    from besdk.work.job import LOCAL, SINGLETON, UNLIMITED, Job

    async def handle(run) -> None:
        await cleanup(db)
        await run.complete()

    return Job("be.idempotency.cleanup", SINGLETON, handle, LOCAL, interval=CLEANUP_EVERY, max_attempts=UNLIMITED)
