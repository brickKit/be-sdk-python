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
_INTEGER = re.compile(r"-?[0-9]+")  # as Go's strconv.ParseInt: no signs but -, no underscores, no spaces


@dataclass
class Shard:
    """One shard of the authorization view (be.security.view)."""

    shard: str
    applicability: list[str]
    subjects: dict[str, tuple[list[Segment], bool]]  # principal -> (segments, accounts disabled)
    digest: str

    @staticmethod
    def from_dict(d: dict) -> Shard:
        subjects = {p: ([Segment.from_dict(s) for s in e.get("segments") or []], bool(e.get("accounts_disabled")))
                    for p, e in (d.get("subjects") or {}).items()}
        return Shard(d["shard"], list(d.get("applicability") or []), subjects, d.get("digest", ""))


class Source(Protocol):
    """Answers verification requests (be.security.view_verify_request -> view_verify_answer)."""

    async def verify(self, request: dict) -> dict: ...


class GRPCSource:
    """Verifies with the authorization member over its system surface (mTLS, the call policy). Until the family
    contract is generated, the request and answer travel as their canonical JSON records in a BytesValue."""

    def __init__(self, channel: Any):
        from google.protobuf import wrappers_pb2

        self._call = channel.unary_unary(VERIFY_METHOD, request_serializer=wrappers_pb2.BytesValue.SerializeToString,
                                         response_deserializer=wrappers_pb2.BytesValue.FromString)
        self._bytes = wrappers_pb2.BytesValue

    async def verify(self, request: dict) -> dict:
        import json

        from besdk.calls.client import options

        out = await self._call(self._bytes(value=json.dumps(request).encode()), metadata=options(idempotent=True))
        return json.loads(out.value)


@dataclass
class _Held:
    shard: Shard
    sent_at: float  # the suspend-aware reading when the request that renewed it was sent


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
    recovery: int = 0
    stopped: bool = False  # a rollback was detected: nothing is used until a valid answer arrives
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
            gen = int(raw)
            if gen < self.recovery:
                self.shards, self.stopped = {}, True
                self.log.error("the authorization member answered with an older recovery generation: a rollback; "
                               "the view is not used", held=self.recovery, answered=gen)
                return
            if gen > self.recovery:
                self.shards, self.recovery = {}, gen  # every older shard is void
            self.stopped = False
            for a in ans.get("shards") or []:
                if a.get("status") == "unchanged" and a.get("shard") in self.shards:
                    self.shards[a["shard"]].sent_at = sent
                elif a.get("status") == "changed" and a.get("view"):
                    self.shards[a["shard"]] = _Held(Shard.from_dict(a["view"]), sent)
                # closed: not renewed, it runs out on its own

    def lookup(self, resource_type: str, principal: str, risk: str) -> Grant:
        """The subject's segments for a resource type in a valid shard; risk high uses W_HIGH."""
        valid = (W_HIGH if risk == "high" else W_READ) * (1 - DRIFT)
        now = self.boot()
        if not self.stopped:
            for h in self.shards.values():
                if resource_type not in h.shard.applicability or now >= h.sent_at + valid:
                    continue
                segs, disabled = h.shard.subjects.get(principal, ([], False))
                mine = [] if disabled else [s for s in segs if not s.resource_type or s.resource_type == resource_type]
                return Grant(mine, h.sent_at + valid)
        raise errs.new("AUTHORIZATION_VIEW_EXPIRED", "no valid authorization view").with_("resource_type", resource_type)
