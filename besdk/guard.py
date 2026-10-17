"""Admits user-surface requests (protocol P8.1-P8.4, P8.12-P8.13): the bearer credential is verified, the route's
operation must be declared, a valid authorization view must exist for it, and the handler runs with the verified
user, its authorization subject and the request's cutoff bound."""
from __future__ import annotations

import contextlib
import time
from datetime import datetime, timezone
from typing import Callable, Iterator

from fastapi import Request

from besdk import clock, errs, reqctx
from besdk.authz import Definitions, Holder, Subject
from besdk.authz.subject import subject_scope
from besdk.http import Route
from besdk.identity import TokenError, User, Verifier, refusal
from besdk.identity.token import user_scope
from besdk.obs import Logger


class UserGuard:
    """Admits requests made with a user's own credential."""

    def __init__(self, verifier: Verifier, defs: Definitions, holder: Holder, log: Logger,
                 boot: Callable[[], float] = clock.since_boot, now: Callable[[], float] = time.time):
        self.verifier, self.defs, self.holder, self.log, self.boot, self.now = verifier, defs, holder, log, boot, now

    async def admit(self, route: Route, request: Request) -> contextlib.AbstractContextManager:
        auth = request.headers.get("authorization", "")
        token = auth[len("Bearer "):] if auth.startswith("Bearer ") else ""
        if not token:
            raise errs.new("TOKEN_INVALID", "the credential is missing or not valid")
        try:
            user = await self.verifier.verify(token)
        except TokenError as e:
            self.log.info("a credential was refused", problem=e.problem)  # never the credential itself
            raise refusal(e) from None
        op = self.defs.operation(route.operation)
        if op is None:
            raise errs.from_exception(ValueError(f"route operation {route.operation} is not declared"))  # type: ignore[misc]
        g = self.holder.lookup(op.resource_type, user.subject, op.risk_class)
        now = self.now()
        cutoff = min(self.boot() + (user.expiry - now), g.cutoff)
        subject = Subject(user.subject, self.defs, self.holder, datetime.fromtimestamp(now, timezone.utc))
        return _bound(user, subject, cutoff)


@contextlib.contextmanager
def _bound(user: User, subject: Subject, cutoff: float) -> Iterator[None]:
    with user_scope(user), subject_scope(subject), reqctx.cutoff_scope(cutoff):
        yield
