"""The bounded local view (protocol P8.9-P8.13; security 03): shards held and verified against the authorization
member, validity counted from when the request left on the suspend-aware clock, stall and rollback detection."""
from __future__ import annotations

import asyncio
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol

from besdk import clock, errs, reqctx
from besdk.authz.segment import Segment
from besdk.obs import Logger

# Parameters of security 03 (starting values).
W_READ, W_HIGH, DRIFT, T_STALL = 30.0, 5.0, 0.001, 2.0
INTERVAL = W_HIGH / 2
VERIFY_METHOD = "/be.authz.v3.ViewService/Verify"
_STATUS = {1: "unchanged", 2: "changed", 3: "closed"}  # be.authz.v3.ShardStatus
_INTEGER = re.compile(r"-?[0-9]+")  # as Go's strconv.ParseInt: no signs but -, no underscores, no spaces


@dataclass
class Shard:
    """One shard of the authorization view (be.security.view)."""

    shard: str
    applicability: list[str]
    subjects: dict[str, tuple[list[Segment], bool]]  # principal -> (segments, accounts disabled)
    digest: str
    security_revisions: list[dict] = field(default_factory=list)  # RevisionRef: domain, value, epoch, instance

    @staticmethod
    def from_dict(d: dict) -> Shard:
        subjects = {p: ([Segment.from_dict(s) for s in e.get("segments") or []], bool(e.get("accounts_disabled")))
                    for p, e in (d.get("subjects") or {}).items()}
        return Shard(d["shard"], list(d.get("applicability") or []), subjects, d.get("digest", ""),
                     list(d.get("security_revisions") or []))


def open_view(raw: bytes, digest: str) -> Shard:
    """Checks a view shard's bytes (canonical, its digest, the schema; D76) and decodes it."""
    from besdk import records

    d = records.open_("be.security.view", raw, digest)
    return Shard.from_dict({**d, "digest": digest})


class Source(Protocol):
    """Answers verification requests (be.security.view_verify_request -> view_verify_answer)."""

    async def verify(self, request: dict) -> dict: ...


class GRPCSource:
    """Verifies with the authorization member over its system surface (mTLS, the call policy; contract-infra-authz).
    A changed shard's bytes are opened as a be.security.view; one that fails discards the whole answer."""

    def __init__(self, channel: Any):
        from be.authz.v3 import authz_pb2_grpc

        self._stub = authz_pb2_grpc.ViewServiceStub(channel)

    async def verify(self, request: dict) -> dict:
        from be.authz.v3 import authz_pb2

        from besdk.calls.client import options

        out = await self._stub.Verify(authz_pb2.VerifyRequest(
            component=request["component"], nonce=request["nonce"],
            held=[authz_pb2.HeldShard(shard=h["shard"], digest=h["digest"]) for h in request["held"]]),
            metadata=options(idempotent=True, background=True))
        rg = out.recovery_generation
        ans: dict = {"nonce": out.nonce, "computed_at": out.computed_at, "instance": out.instance,
                     "recovery_generation": {"domain": rg.domain, "value": rg.value, "epoch": rg.epoch,
                                             "instance": rg.instance}, "shards": []}
        for s in out.shards:
            status = _STATUS.get(s.status)
            if status is None:
                raise ValueError(f"shard {s.shard}: status {s.status}")
            a: dict = {"shard": s.shard, "status": status}
            if status == "changed":
                view = open_view(s.view, s.digest)
                if view.shard != s.shard:
                    raise ValueError(f"shard {s.shard}: the view is for shard {view.shard}")
                a["view"] = view
            ans["shards"].append(a)
        return ans


def _epoch(e: str) -> tuple[int, int]:
    g, _, t = (e or "0.0").partition(".")
    return int(g or 0), int(t or 0)


def _goes_back(held: list[dict], new: list[dict]) -> bool:
    """One of the new revisions has an earlier epoch, or the same epoch and a lower value, than the held revision of
    its domain."""
    old = {r.get("domain"): r for r in held}
    for r in new:
        o = old.get(r.get("domain"))
        if o is None:
            continue
        if _epoch(r.get("epoch", "")) < _epoch(o.get("epoch", "")):
            return True
        a, b = r.get("value", ""), o.get("value", "")
        if r.get("epoch", "") == o.get("epoch", "") and _INTEGER.fullmatch(a) and _INTEGER.fullmatch(b) and int(a) < int(b):
            return True
    return False


@dataclass
class _Held:
    shard: Shard
    sent_at: float  # the suspend-aware reading when the request that renewed it was sent
    stopped: bool = False  # its revisions went back under the same recovery generation: unusable until a higher one
    suspended: bool = False  # a whole answer was a rollback: unusable until an accepted answer renews or replaces it


@dataclass
class Grant:
    """What a lookup gives a request: the subject's segments for a resource type and the cutoff."""

    segments: list[Segment]
    cutoff: float  # the suspend-aware reading after which the evidence is no longer valid


@dataclass
class Holder:
    """A member's view shards and their validity. One per member; after a start there is no trusted view (P8.10)."""

    component: str
    source: Source
    log: Logger
    boot: Callable[[], float] = clock.since_boot  # replaceable in tests
    wall: Callable[[], float] = time.time
    shards: dict[str, _Held] = field(default_factory=dict)
    recovery: int | None = None  # None until the first accepted answer
    instance: str = ""  # the member instance of the first accepted answer (P8.11)
    _last_tick: float | None = None
    _last_skew: float = 0.0
    _verifying: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def run(self, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> None:
        """Verifies every INTERVAL until cancelled, and detects stalls."""
        while True:
            self.tick()
            try:
                # calls need a deadline; a verification never outlives its period
                with reqctx.deadline_scope(time.monotonic() + INTERVAL):
                    await self.verify()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - the view stays valid only until it expires
                self.log.warn("authorization view verification failed; the view stays valid only until it expires",
                              error=str(e))
            self.rest()
            await sleep(INTERVAL)

    def rest(self) -> None:
        """Marks the start of the wait between verifications: the scheduling gap tick() checks is the wait alone, so a
        slow verification (its own deadline is INTERVAL) is never taken for a stall."""
        now = self.boot()
        self._last_tick, self._last_skew = now, self.wall() - now

    def tick(self) -> None:
        """A scheduling gap beyond INTERVAL + T_STALL since rest(), or a jump between the boot clock and the wall
        clock beyond T_STALL, invalidates every shard (P8.10)."""
        now, wall = self.boot(), self.wall()
        skew = wall - now
        if self._last_tick is not None:
            gap, jump = now - self._last_tick, skew - self._last_skew
            if gap > INTERVAL + T_STALL or abs(jump) > T_STALL:
                if self.shards:
                    self.log.warn("a stall was detected; every authorization view shard is invalidated",
                                  gap_ms=int(gap * 1000))
                self.shards = {}
        self._last_tick, self._last_skew = now, skew

    async def verify(self) -> None:
        """Sends one verification request and applies the answer (P8.9, P8.11)."""
        async with self._verifying:
            nonce = secrets.token_hex(16)
            req = {"component": self.component, "nonce": nonce,
                   "held": [{"shard": h.shard.shard, "digest": h.shard.digest} for h in self.shards.values()]}
            sent = self.boot()  # validity counts from the moment the request leaves
            ans = await self.source.verify(req)
            if ans.get("nonce") != nonce:
                raise ValueError("an answer for another request (nonce mismatch) was discarded")
            raw = (ans.get("recovery_generation") or {}).get("value", "")
            if not isinstance(raw, str) or not _INTEGER.fullmatch(raw):
                raise ValueError(f"recovery generation {raw!r}")
            self.apply(ans, int(raw), sent)

    def apply(self, ans: dict, gen: int, sent: float) -> bool:
        """Security 03's acceptance table (P8.11), in order: a rollback of the whole answer, a higher recovery
        generation, then shard by shard. Returns whether the answer was accepted."""
        instance = ans.get("instance", "")
        views = [_as_shard(a["view"]) for a in ans.get("shards") or [] if a.get("view") is not None]
        foreign = (self.instance != "" and instance != self.instance) or any(
            r.get("instance", "") != instance for v in views for r in v.security_revisions)
        if (self.recovery is not None and gen < self.recovery) or foreign:
            for h in self.shards.values():
                h.suspended = True
            self.log.error("an authorization view answer was a rollback (an older recovery generation or another member "
                           "instance); the views are suspended", held_generation=self.recovery, answered_generation=gen,
                           held_instance=self.instance, answered_instance=instance)
            return False
        newer = self.recovery is None or gen > self.recovery
        if newer:
            self.shards = {}  # every older shard is void, the stopped ones too
        self.recovery, self.instance = gen, instance
        for a in ans.get("shards") or []:
            name, h = a.get("shard"), self.shards.get(a.get("shard"))
            if a.get("status") == "unchanged":
                if h is not None and not h.stopped:
                    h.sent_at, h.suspended = sent, False
            elif a.get("status") == "changed" and a.get("view") is not None:
                view = _as_shard(a["view"])
                if h is not None and h.stopped:
                    continue
                if h is not None and not newer and _goes_back(h.shard.security_revisions, view.security_revisions):
                    h.stopped = True
                    self.log.error("an authorization view shard went back under the same recovery generation; it is "
                                   "stopped until the recovery generation advances", shard=name)
                else:
                    self.shards[name] = _Held(view, sent)
            # closed: not renewed, it runs out on its own
        return True

    def lookup(self, resource_type: str, principal: str, risk: str) -> Grant:
        """The subject's segments for a resource type in a valid shard; risk high uses W_HIGH."""
        valid = (W_HIGH if risk == "high" else W_READ) * (1 - DRIFT)
        now = self.boot()
        for h in self.shards.values():
            if h.stopped or h.suspended or resource_type not in h.shard.applicability or now >= h.sent_at + valid:
                continue
            segs, disabled = h.shard.subjects.get(principal, ([], False))
            mine = [] if disabled else [s for s in segs if not s.resource_type or s.resource_type == resource_type]
            return Grant(mine, h.sent_at + valid)
        raise errs.new("AUTHORIZATION_VIEW_EXPIRED", "no valid authorization view").with_("resource_type", resource_type)


def _as_shard(v: Shard | dict) -> Shard:
    return v if isinstance(v, Shard) else Shard.from_dict(v)
