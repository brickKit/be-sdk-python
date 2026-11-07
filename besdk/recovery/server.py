"""be.recovery.v1.CheckService for one member (P14.1-P14.5): only the recovery tool calls it, in the verifying phase.
Check runs the SDK's checks then the component's, each reading through a transaction that is always rolled back;
Present answers which audited objects are still there and at which revision; Numbers and AdvanceNumbers read and raise
the continuous document number series, the skipped numbers voided by the owner in the raising transaction."""
from __future__ import annotations

from besdk import errs
from besdk._gen.be.recovery.v1 import check_pb2, check_pb2_grpc
from besdk.db.docno import Series, continuous_maxima, raise_continuous
from besdk.db.pool import DB, Tx
from besdk.recovery.checks import ABSENT, OLDER, PRESENT, UNPROVABLE, Checks, Result, builtins


class _ReadOnly(Exception):
    """Rolls a check's transaction back: checks never write."""


def _need_recovery(recovery: str) -> None:
    if not recovery:
        raise errs.new("REQUEST_INVALID", "recovery is required")


class CheckServer(check_pb2_grpc.CheckServiceServicer):
    def __init__(self, db: DB, subscribes: bool, reg: Checks | None = None):
        self.db, self.reg = db, reg or Checks()
        self.checks = builtins(subscribes) + self.reg.checks

    async def _read(self, fn) -> None:
        """Runs fn in a background transaction that is always rolled back."""
        async def body(tx: Tx) -> None:
            await fn(tx)
            raise _ReadOnly

        try:
            await self.db.background_tx(body)
        except Exception as e:  # the transaction entry maps what body raised; the rollback is ours
            if not (isinstance(e, _ReadOnly) or isinstance(e.__cause__, _ReadOnly)):
                raise

    async def Check(self, request, context) -> check_pb2.CheckResponse:  # noqa: N802
        """Every check (P14.1, P14.2); a check that fails to run answers unprovable with the error, so one broken
        check does not hide the others."""
        _need_recovery(request.recovery)
        out = check_pb2.CheckResponse()
        for ch in self.checks:
            try:
                res = await ch.run(self._read)
            except Exception as e:  # noqa: BLE001 - reported as unprovable
                res = Result(UNPROVABLE, disposal=f"the check could not run: {e}")
            out.results.append(check_pb2.CheckResult(
                check=ch.name, basis=ch.basis, outcome=res.outcome, disposal=res.disposal,
                differences=[check_pb2.Difference(object_type=d.object_type, object_id=d.object_id, detail=d.detail)
                             for d in res.differences]))
        return out

    async def Present(self, request, context) -> check_pb2.PresentResponse:  # noqa: N802
        """Whether each object is still in the database and at which revision (P14.3). An object type without a
        registered presence question answers CAPABILITY_UNSUPPORTED; a version that is not a revision number,
        REQUEST_INVALID."""
        _need_recovery(request.recovery)
        by_type: dict[str, list[str]] = {}
        for o in request.objects:
            if o.type not in self.reg.presence:
                raise errs.new("CAPABILITY_UNSUPPORTED", "no presence question for the object type " + o.type)
            if o.version and not o.version.lstrip("-").isdigit():
                raise errs.new("REQUEST_INVALID", f"version {o.version} is not a revision number")
            by_type.setdefault(o.type, []).append(o.id)
        found: dict[str, dict[str, int]] = {}

        async def read(tx: Tx) -> None:
            for t in sorted(by_type):
                found[t] = await self.reg.presence[t](tx, by_type[t])

        await self._read(read)
        out = check_pb2.PresentResponse()
        for o in request.objects:
            a = check_pb2.PresentAnswer(object=o, answer=ABSENT)
            if (rev := found.get(o.type, {}).get(o.id)) is not None:
                a.answer, a.version = PRESENT, str(rev)
                if o.version and rev < int(o.version):
                    a.answer = OLDER
            out.answers.append(a)
        return out

    async def Numbers(self, request, context) -> check_pb2.NumbersResponse:  # noqa: N802
        """The continuous series and their used maximum (P14.5)."""
        _need_recovery(request.recovery)
        maxima = []

        async def read(tx: Tx) -> None:
            maxima.extend(await continuous_maxima(tx))

        await self._read(read)
        return check_pb2.NumbersResponse(series=[check_pb2.Series(
            legal_entity=u.series.legal_entity, code=u.series.code, period=u.series.period, max_used=u.max)
            for u in maxima])

    async def AdvanceNumbers(self, request, context) -> check_pb2.AdvanceNumbersResponse:  # noqa: N802
        """Raises each series' used maximum (never lowers it) and has the component void the numbers it skips over,
        all in one transaction (P14.5); answers each series' maximum after."""
        _need_recovery(request.recovery)

        async def body(tx: Tx) -> list[check_pb2.Series]:
            out = []
            for s in request.series:
                if s.max_used < 0:
                    raise errs.new("REQUEST_INVALID", "max_used is negative")
                sr = Series(s.legal_entity, s.code, s.period)
                before = await raise_continuous(tx, sr, s.max_used)
                after = max(before, s.max_used)
                if after > before:
                    if self.reg.void is None:
                        raise errs.new("CAPABILITY_UNSUPPORTED", "this component registers no voiding of skipped "
                                                                 "numbers (Checks.set_void)")
                    await self.reg.void(tx, sr, before + 1, after)
                out.append(check_pb2.Series(legal_entity=sr.legal_entity, code=sr.code, period=sr.period,
                                            max_used=after))
            return out

        return check_pb2.AdvanceNumbersResponse(series=await self.db.background_tx(body))
