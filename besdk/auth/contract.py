"""The resource contract a component mounts when it declares ``resources`` (be-protocol P6.10, P6.11):

- ``POST /{d}/{n}/_authz/check`` — up to 500 ``(key, type, id)`` decisions for the caller (E10);
- ``GET /{d}/{n}/_authz/explain?key=&type=&id=`` — the facts (E12); 404 for a record the caller cannot see,
  indistinguishable from one that does not exist (R62);
- ``GET | POST /{d}/{n}/_shares/{type}/{id}`` and ``DELETE …/{share_id}`` — shares held by the provider
  (capability ``sharing``, else 501), written through its WriteTuples; a write answers after the local
  projection reached the provider's revision.

The component tells the SDK how to read one record's facts with ``SharingLoader(type, load)``.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from fastapi import Body, Header, Request

from besdk import errors
from besdk.auth import evaluate as E
from besdk.auth import provider
from besdk.auth.access import AUTHENTICATED, access

if TYPE_CHECKING:
    from besdk.http.router import Router
    from besdk.runtime import Runtime

MAX_CHECKS = 500
SHARE_WAIT = 3.0


@dataclass(frozen=True)
class SharingLoader:
    """``load(tx, id)`` returns the record's facts as a ``Row`` (owner, dept_path, dimension values) or None."""

    type: str
    load: Callable[[Any, str], Awaitable[E.Row | None]]


def share_id(relation: str, subject: str) -> str:
    return base64.urlsafe_b64encode(json.dumps([relation, subject]).encode()).decode().rstrip("=")


def parse_share_id(sid: str) -> tuple[str, str]:
    try:
        relation, subject = json.loads(base64.urlsafe_b64decode(sid + "=" * (-len(sid) % 4)))
        return str(relation), str(subject)
    except (ValueError, TypeError):
        raise errors.be_error("NOT_FOUND") from None


class ResourceContract:
    def __init__(self, rt: "Runtime", loaders: list[SharingLoader]):
        self.rt = rt
        self.loaders = {x.type: x for x in loaders}
        for t in rt.resources:
            if t not in self.loaders:
                rt.logger.error("sharing_loader_missing", extra={"type": t})

    async def _row(self, rtype: str, rid: str) -> E.Row | None:
        loader = self.loaders.get(rtype)
        if loader is None or rtype not in self.rt.resources:
            return None
        return await self.rt.store().tx(lambda tx: loader.load(tx, rid))

    async def decide(self, key: str, rtype: str, rid: str) -> E.Decision:
        row = await self._row(rtype, rid)
        if row is None:
            return E.Decision(False, False, "NOT_FOUND")
        return await access().can(key, rtype, row)

    async def visible_row(self, rtype: str, rid: str) -> E.Row:
        """The record when the caller can see it (view_key), else 404 (R62)."""
        row = await self._row(rtype, rid)
        if row is None or not (await access().can(self.rt.resources[rtype].rtype.view_key, rtype, row)).visible:
            raise errors.be_error("NOT_FOUND")
        return row

    def sharing_on(self) -> None:
        src = self.rt.shared.bundle_source
        if src is None or src.bundle is None or not src.bundle.capability("sharing"):
            raise errors.be_error("CAPABILITY_UNAVAILABLE", {"capability": "sharing"})

    # --- shares ------------------------------------------------------------------------------------

    def _share_rules(self, rtype: str, relation: str, subject: str) -> str:
        """The type's share key, after checking the relation and the subject kind may be shared."""
        rt = self.rt.resources[rtype].rtype
        rule = rt.share or {}
        if not rule.get("key"):
            raise errors.be_error("SHARE_NOT_ALLOWED", {"type": rtype})
        a = access()
        if not a.has(rule["key"]):
            raise errors.be_error("MISSING_PERMISSION", {"permission": rule["key"]})
        kind = subject.split(":", 1)[0]
        if relation not in (rule.get("relations") or ()) or kind not in (rule.get("subjects") or ()) \
                or rt.capability_of(relation) != "sharing":
            raise errors.be_error("SHARE_NOT_ALLOWED", {"relation": relation, "subject": kind})
        return rule["key"]

    async def list_shares(self, rtype: str, rid: str) -> list[dict]:
        rt = self.rt.resources[rtype].rtype
        rows = await self.rt.store().tx(lambda tx: tx.fetch(
            "SELECT relation, subject, expires_at FROM besdk_authz_acl WHERE rtype = $1 AND rid = $2 "
            "AND (expires_at IS NULL OR expires_at > now()) ORDER BY relation, subject", rtype, rid))
        return [{"share_id": share_id(r["relation"], r["subject"]), "subject": r["subject"],
                 "relation": r["relation"], "expires_at": r["expires_at"].isoformat() if r["expires_at"] else None}
                for r in rows if rt.capability_of(r["relation"]) == "sharing"]

    async def write(self, *, writes: list = (), deletes: list = (), key: str = "") -> str:
        a = access()
        revision = await provider.write_tuples(self.rt, writes=writes, deletes=deletes, idempotency_key=key,
                                               actor_sub=a.user.sub, act=a.user.act)
        proj = self.rt.projection
        if proj is not None and not await proj.catch_up(int(revision), budget=SHARE_WAIT):
            self.rt.logger.warning("share_projection_behind", extra={"revision": revision})
        return revision


def mount(r: "Router", c: ResourceContract) -> None:
    @r.post("/_authz/check", guard=AUTHENTICATED)
    async def authz_check(body: dict = Body(...)) -> dict:
        checks = body.get("checks")
        if not isinstance(checks, list):
            raise errors.be_error("REQUEST_INVALID", message="checks is an array")
        if len(checks) > MAX_CHECKS:
            raise errors.be_error("BATCH_TOO_LARGE", {"field": "checks", "max": str(MAX_CHECKS), "got": str(len(checks))})
        out = []
        for ch in checks:
            d = await c.decide(str(ch.get("key", "")), str(ch.get("type", "")), str(ch.get("id", "")))
            out.append({"visible": d.visible, "allowed": d.allowed, "reason": d.reason})
        return {"results": out}

    @r.get("/_authz/explain", guard=AUTHENTICATED)
    async def authz_explain(key: str, type: str, id: str) -> dict:  # noqa: A002 - the contract's names
        row = await c.visible_row(type, id)
        a = access()
        rt = c.rt.resources[type].rtype
        facts = E.explain(a.principal, key, rt, row, acl=await a.acl(type, id))
        d = E.decide(a.principal, key, rt, row, acl=await a.acl(type, id))
        return {"decision": "allowed" if d.allowed else "visible", **facts}

    @r.get("/_shares/{type}/{id}", guard=AUTHENTICATED)
    async def shares_list(type: str, id: str) -> dict:  # noqa: A002
        c.sharing_on()
        await c.visible_row(type, id)
        return {"shares": await c.list_shares(type, id)}

    @r.post("/_shares/{type}/{id}", guard=AUTHENTICATED)
    async def shares_create(type: str, id: str, request: Request, body: dict = Body(...),  # noqa: A002
                            idempotency_key: str | None = Header(None)) -> dict:
        c.sharing_on()
        await c.visible_row(type, id)
        subject, relation = str(body.get("subject", "")), str(body.get("relation", ""))
        c._share_rules(type, relation, subject)
        exp = body.get("expires_at")
        expires = datetime.fromisoformat(exp.replace("Z", "+00:00")) if isinstance(exp, str) and exp else None
        from besdk.idem import resolve_key

        key = resolve_key(idempotency_key, body.get("idempotency_key")) or ""
        rev = await c.write(writes=[provider.tuple_msg(type, id, relation, subject, expires)], key=key)
        share = {"share_id": share_id(relation, subject), "subject": subject, "relation": relation,
                 "expires_at": exp or None, "created_by": access().user.sub}
        return {"share": share, "revision": rev}

    @r.delete("/_shares/{type}/{id}/{sid}", guard=AUTHENTICATED)
    async def shares_delete(type: str, id: str, sid: str) -> dict:  # noqa: A002
        c.sharing_on()
        await c.visible_row(type, id)
        relation, subject = parse_share_id(sid)
        c._share_rules(type, relation, subject)
        return {"revision": await c.write(deletes=[provider.tuple_msg(type, id, relation, subject)])}
