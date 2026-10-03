"""``besdk.Router``: the user-plane routes of one component under ``/{domain}/{name}`` (be-protocol P3.1).

Every route declares exactly one guard (P6.2) — a missing ``guard`` is a ``TypeError`` when the module
is loaded — and may declare its own deadline and body limit (P3.4, P3.6, P3.13). The route class wraps
each handler with the decision chain, the deadline and the body limit.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Callable

from fastapi import APIRouter, Request
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from starlette.responses import Response

from besdk import context
from besdk.auth.access import PUBLIC, PermKey
from besdk.errors import Code, Error, Violation, be_error

DEFAULT_BODY_LIMIT = 1024 * 1024


@dataclass(frozen=True)
class RouteMeta:
    guard: PermKey
    timeout: float | None = None
    body_limit: int | None = None


class Router:
    """Wraps a FastAPI ``APIRouter``; ``@r.get(path, guard=…, timeout=…, body_limit=…)``."""

    def __init__(self, component_id: str):
        self.prefix = "/" + component_id
        self.api = APIRouter(prefix=self.prefix, route_class=BeRoute)
        self.metas: list[RouteMeta] = []

    def _route(self, method: str, path: str, *, guard: PermKey, timeout: float | None = None,
               body_limit: int | None = None, **kw: Any) -> Callable:
        if not isinstance(guard, PermKey):
            raise TypeError("guard must be a PermKey, PUBLIC or AUTHENTICATED")
        meta = RouteMeta(guard, timeout, body_limit)
        self.metas.append(meta)
        extra = dict(kw.pop("openapi_extra", None) or {})
        extra["x-be-guard"] = str(guard)
        if timeout:
            extra["x-be-deadline-seconds"] = timeout
        if body_limit:
            extra["x-be-max-body-bytes"] = body_limit
        return self.api.api_route(path, methods=[method], openapi_extra=extra, **kw)

    def get(self, path: str, *, guard: PermKey, **kw: Any) -> Callable:
        return self._route("GET", path, guard=guard, **kw)

    def post(self, path: str, *, guard: PermKey, **kw: Any) -> Callable:
        return self._route("POST", path, guard=guard, **kw)

    def put(self, path: str, *, guard: PermKey, **kw: Any) -> Callable:
        return self._route("PUT", path, guard=guard, **kw)

    def patch(self, path: str, *, guard: PermKey, **kw: Any) -> Callable:
        return self._route("PATCH", path, guard=guard, **kw)

    def delete(self, path: str, *, guard: PermKey, **kw: Any) -> Callable:
        return self._route("DELETE", path, guard=guard, **kw)

    @property
    def protected(self) -> bool:
        return any(m.guard != PUBLIC for m in self.metas)


def _validation_error(e: RequestValidationError) -> Error:
    vs = [Violation(".".join(str(p) for p in err.get("loc", ())), str(err.get("type", "invalid")).upper(),
                    str(err.get("msg", ""))) for err in e.errors()]
    return Error(Code.INVALID_ARGUMENT, "REQUEST_INVALID", message="the request does not match the contract",
                 violations=vs)


def _limit_receive(request: Request, limit: int) -> None:
    """Count the body as it arrives; past the limit the read fails with 413 (P3.6)."""
    receive, seen = request._receive, 0

    async def counted():
        nonlocal seen
        msg = await receive()
        if msg["type"] == "http.request":
            seen += len(msg.get("body", b""))
            if seen > limit:
                raise be_error("BODY_TOO_LARGE", {"limit": str(limit)})
        return msg

    request._receive = counted


async def _decide(rt: Any, guard: PermKey, authorization: str | None) -> Any:
    """The decision chain (P6.2); refusals are counted in ``be_authz_denied_total{reason}``."""
    if guard == PUBLIC:
        return None
    try:
        return await rt.authorizer().decide(guard, authorization)
    except Error as e:
        rt.metrics.authz_denied.labels(reason=e.reason or e.code.name).inc()
        raise


def _meta_of(extra: dict) -> RouteMeta:
    """The route's guard, deadline and body limit, carried in its OpenAPI extensions (P3.13)."""
    return RouteMeta(PermKey(extra.get("x-be-guard", "")), extra.get("x-be-deadline-seconds"),
                     extra.get("x-be-max-body-bytes"))


class BeRoute(APIRoute):
    """Runs the decision chain, the route deadline and the body limit around the FastAPI handler."""

    def get_route_handler(self) -> Callable:
        inner = super().get_route_handler()

        async def handler(request: Request) -> Response:
            meta = _meta_of(self.openapi_extra or {})
            rt = request.app.state.rt
            req = request.scope.setdefault("be", {})
            limit = meta.body_limit or DEFAULT_BODY_LIMIT
            if int(request.headers.get("content-length") or 0) > limit:
                raise be_error("BODY_TOO_LARGE", {"limit": str(limit)})
            _limit_receive(request, limit)
            seconds = meta.timeout or rt.http_default_timeout
            deadline = time.monotonic() + seconds
            req["perm"] = "" if meta.guard in (PUBLIC,) else str(meta.guard)
            with context.scope(deadline=deadline, perm=req["perm"], req=req):
                access = await _decide(rt, meta.guard, request.headers.get("authorization"))
                user = access.user if access else None
                if user:
                    req["sub"] = user.sub
                token = (request.headers.get("authorization") or "")[7:] if user else ""
                with context.scope(access=access, sub=user.sub if user else "", act=user.act if user else None,
                                   token=token, authz_revision=request.headers.get("x-authz-revision", "")):
                    try:
                        async with asyncio.timeout(seconds):
                            return await inner(request)
                    except RequestValidationError as e:
                        raise _validation_error(e) from None
                    except TimeoutError:
                        raise be_error("STATEMENT_TIMEOUT" if req.get("db_active") else "DEADLINE_BUDGET_EXHAUSTED") \
                            from None

        return handler
