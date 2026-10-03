"""The ACL projection (be-protocol P6.11, P6.12): the direct tuples of the component's own resource types (and
the types they inherit from) copied from the provider's changefeed into ``besdk_authz_acl``.

``pull`` reads ``GET {AUTHZ_URL}/authz/v2/changes?types=…&after=<cursor>&limit=500`` page by page; each page
is applied in one transaction that locks the cursor row and skips changes at or below it, so replicas
pulling at the same time never move the projection backwards. A page without our changes still advances
the cursor to the watermark. ``410`` rebuilds from ``/authz/v2/tuples`` and continues from the snapshot's
revision; ``501`` (no ``sharing`` capability) leaves the projection empty. Run as the singleton job
``be.authz.changes`` every 5 s and at once on a poke.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING, Any

import httpx

from besdk.auth.resources import pulled_types

if TYPE_CHECKING:
    from besdk.runtime import Runtime

PAGE, TIMEOUT = 500, 3.0

_ENSURE = "INSERT INTO besdk_authz_cursor (scope, revision) VALUES ($1, 0) ON CONFLICT (scope) DO NOTHING"
_CURSOR = "SELECT revision FROM besdk_authz_cursor WHERE scope = $1 FOR UPDATE"
_UPSERT = ("INSERT INTO besdk_authz_acl (rtype, rid, relation, subject, expires_at, revision) "
           "VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (rtype, rid, relation, subject) "
           "DO UPDATE SET expires_at = EXCLUDED.expires_at, revision = EXCLUDED.revision")
_DELETE = "DELETE FROM besdk_authz_acl WHERE rtype = $1 AND rid = $2 AND relation = $3 AND subject = $4"
_ADVANCE = "UPDATE besdk_authz_cursor SET revision = greatest(revision, $2) WHERE scope = $1"
_REBUILT = "UPDATE besdk_authz_cursor SET revision = $2, rebuilt_at = now() WHERE scope = $1"


class Unavailable(Exception):
    """The provider lacks the capability (501) or cannot be reached."""


def _ts(v: Any) -> datetime | None:
    return datetime.fromisoformat(v.replace("Z", "+00:00")) if isinstance(v, str) and v else None


def _row(t: dict, revision: int) -> tuple:
    o = t["object"]
    return (o["type"], str(o["id"]), t["relation"], t["subject"], _ts(t.get("expires_at")), revision)


class Projection:
    def __init__(self, rt: "Runtime", *, page: int = PAGE):
        self.rt, self.page = rt, page
        self.types = pulled_types(rt.resources)
        self.scope = ",".join(self.types)
        self.base = (rt.config.family("AUTHZ_URL") or "").rstrip("/")
        self._lock = asyncio.Lock()

    async def _get(self, path: str, params: dict) -> httpx.Response:
        r = await self.rt.shared.http.get(self.base + path, params=params, timeout=TIMEOUT,
                                          headers={"be-caller": self.rt.id})
        if r.status_code in (501, 404):
            raise Unavailable(f"{path} answered {r.status_code}")
        return r

    async def watermark(self) -> int:
        v = await self.rt.store().tx(lambda tx: tx.fetchval(
            "SELECT revision FROM besdk_authz_cursor WHERE scope = $1", self.scope))
        return int(v or 0)

    async def pull(self) -> None:
        """Apply every change available now; a provider without the capability is a no-op."""
        async with self._lock:
            try:
                while await self._page():
                    pass
            except Unavailable as e:
                self.rt.logger.debug("authz_changes_unavailable", extra={"error": str(e)})

    async def _page(self) -> bool:
        """One page; True when another page may follow."""
        after = await self.watermark()
        r = await self._get("/authz/v2/changes", {"types": self.scope, "after": str(after), "limit": str(self.page)})
        if r.status_code == 410:
            await self.rebuild()
            return True
        r.raise_for_status()
        doc = r.json()
        changes = doc.get("changes") or []
        head = int(doc["next"]) if changes else max(int(doc.get("next") or after), int(doc.get("watermark") or after))
        await self.rt.store().tx(lambda tx: self._apply(tx, changes, head))
        return len(changes) >= self.page

    async def _apply(self, tx: Any, changes: list[dict], head: int) -> None:
        await tx.execute(_ENSURE, self.scope)
        cur = int(await tx.fetchval(_CURSOR, self.scope) or 0)
        for c in changes:
            rev = int(c["revision"])
            if rev <= cur:
                continue
            row = _row(c["tuple"], rev)
            if c["op"] == "delete":
                await tx.execute(_DELETE, *row[:4])
            else:
                await tx.execute(_UPSERT, *row)
        await tx.execute(_ADVANCE, self.scope, head)

    async def rebuild(self) -> None:
        """410: replace the projection with the provider's snapshot, then continue from its revision."""
        rows, revisions = [], []
        for t in self.types:
            cursor = ""
            while True:
                r = await self._get("/authz/v2/tuples", {"type": t, "cursor": cursor, "page_size": "1000"})
                r.raise_for_status()
                doc = r.json()
                revisions.append(int(doc["revision"]))
                rows += [_row(x, int(doc["revision"])) for x in doc.get("tuples") or ()]
                cursor = doc.get("next_cursor") or ""
                if not cursor:
                    break
        rev = min(revisions) if revisions else 0

        async def replace(tx: Any) -> None:
            await tx.execute(_ENSURE, self.scope)
            await tx.fetchval(_CURSOR, self.scope)
            await tx.execute("DELETE FROM besdk_authz_acl WHERE rtype = ANY($1::text[])", self.types)
            if rows:
                await tx.executemany(_UPSERT, rows)
            await tx.execute(_REBUILT, self.scope, rev)

        await self.rt.store().tx(replace)
        self.rt.logger.warning("authz_projection_rebuilt", extra={"revision": rev, "tuples": len(rows)})

    async def catch_up(self, revision: int, *, budget: float = 0.3) -> bool:
        """P6.11: reach ``revision`` within ``budget`` seconds; False when still behind."""
        if await self.watermark() >= revision:
            return True
        try:
            async with asyncio.timeout(budget):
                await self.pull()
        except TimeoutError:
            pass
        except (httpx.HTTPError, Unavailable):
            return False
        return await self.watermark() >= revision

