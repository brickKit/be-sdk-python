"""The tiering's administration on the user surface (be-protocol P15.40): holds and destruction approvals under each
class's declared operations, and an export's status for its requester under the class reader's operation. Every
component with classes has the same routes; none writes its own."""
from __future__ import annotations

import datetime as dt

from fastapi.responses import JSONResponse, Response

from besdk import errs
from besdk.http import Context, Routes
from besdk.tiering import history, holds
from besdk.tiering.catalog import Catalog

ROUTE_PREFIX = "/be/tiering/"  # under the component's own prefix


def _subject():
    from besdk.authz.subject import subject

    s = subject()
    if s is None:
        raise errs.new("TOKEN_INVALID", "no verified subject")
    return s


async def _scope_body(c: Context) -> dict:
    b = await c.json()
    if not isinstance(b, dict):
        raise errs.new("REQUEST_INVALID", "the body is not an object")
    for k in ("scope_kind", "scope", "source", "reason"):
        if not isinstance(b.get(k, ""), str):
            raise errs.new("REQUEST_INVALID", f"{k} is not a string")
    return b


def routes(cat: Catalog, r: Routes) -> None:
    """Registers the administration routes of every class (holds and approvals where the class declares their
    operations) and the export status of every class with a reader: register the readers first."""
    for name in sorted(cat.classes):
        cl, base = cat.classes[name], ROUTE_PREFIX + name
        if cl.hold_operation:
            async def set_hold(c: Context, name: str = name) -> Response:
                b = await _scope_body(c)
                hid = await holds.hold(cat, _subject(), name, holds.Scope(b.get("scope_kind", ""), b.get("scope", "")),
                                       b.get("source", ""), b.get("reason", ""))
                return JSONResponse({"id": hid}, status_code=201)

            async def lift(c: Context, name: str = name) -> Response:
                await holds.lift_hold(cat, _subject(), name, c.param("id"))
                return Response(status_code=204)

            r.post(base + "/holds", cl.hold_operation, set_hold)
            r.post(base + "/holds/{id}/lift", cl.hold_operation, lift)
        if cl.approve_operation:
            async def give(c: Context, name: str = name) -> Response:
                b = await _scope_body(c)
                aid = await holds.approve(cat, _subject(), name, holds.Scope(b.get("scope_kind", ""), b.get("scope", "")),
                                          b.get("reason", ""))
                return JSONResponse({"id": aid}, status_code=201)

            async def withdraw(c: Context, name: str = name) -> Response:
                await holds.withdraw(cat, _subject(), name, c.param("id"))
                return Response(status_code=204)

            r.post(base + "/approvals", cl.approve_operation, give)
            r.post(base + "/approvals/{id}/withdraw", cl.approve_operation, withdraw)
        rd = cat.readers.get(name)
        if rd is not None:
            async def export(c: Context) -> Response:
                st = await history.export_status(cat, c.param("id"), _subject().principal)
                out: dict = {"state": st.state, "rows": st.rows}
                if st.reason:
                    out["reason"] = st.reason
                if st.url:
                    out["url"] = st.url
                    out["expires_at"] = st.expiry.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                return JSONResponse(out)

            r.get(base + "/exports/{id}", rd.op, export)
