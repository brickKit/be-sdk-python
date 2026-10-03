"""besdk errors ↔ gRPC status with ``google.rpc`` details (be-protocol P4.2)."""

from __future__ import annotations

import math

import grpc
from google.protobuf import duration_pb2
from google.rpc import code_pb2, error_details_pb2, status_pb2

from besdk import errors
from besdk.errors import Code, Error, Violation

DETAILS_KEY = "grpc-status-details-bin"


def grpc_code(code: Code) -> grpc.StatusCode:
    return next(s for s in grpc.StatusCode if s.value[0] == int(code))


def to_status(err: Error, *, component_domain: str | None, message: str) -> status_pb2.Status:
    """ErrorInfo always; BadRequest and RetryInfo when they apply. ``err`` is already the visible error."""
    st = status_pb2.Status(code=int(err.code), message=message)
    info = error_details_pb2.ErrorInfo(reason=err.reason or "", domain=err.domain or component_domain or "",
                                       metadata=err.metadata)
    st.details.add().Pack(info)
    if err.violations:
        br = error_details_pb2.BadRequest(field_violations=[
            error_details_pb2.BadRequest.FieldViolation(field=v.field, description=v.description, reason=v.reason)
            for v in err.violations])
        st.details.add().Pack(br)
    if err.retry_after is not None:
        secs = math.ceil(err.retry_after)
        st.details.add().Pack(error_details_pb2.RetryInfo(retry_delay=duration_pb2.Duration(seconds=secs)))
    return st


def trailing(st: status_pb2.Status) -> tuple[tuple[str, bytes], ...]:
    return ((DETAILS_KEY, st.SerializeToString()),)


def from_rpc_error(e: grpc.aio.AioRpcError) -> Error:
    """A dependency's gRPC error as the caller sees it: reason, domain and metadata kept (P4.2)."""
    code = Code(e.code().value[0])
    err = Error(code, None, message=e.details() or "")
    for k, v in e.trailing_metadata() or ():
        if k == DETAILS_KEY:
            st = status_pb2.Status.FromString(v if isinstance(v, bytes) else v.encode())
            for d in st.details:
                if d.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
                    info = error_details_pb2.ErrorInfo()
                    d.Unpack(info)
                    err.reason, err.domain, err.metadata = info.reason or None, info.domain or None, dict(info.metadata)
                elif d.Is(error_details_pb2.BadRequest.DESCRIPTOR):
                    br = error_details_pb2.BadRequest()
                    d.Unpack(br)
                    err.violations = tuple(Violation(f.field, f.reason, f.description) for f in br.field_violations)
                elif d.Is(error_details_pb2.RetryInfo.DESCRIPTOR):
                    ri = error_details_pb2.RetryInfo()
                    d.Unpack(ri)
                    err.retry_after = ri.retry_delay.seconds + ri.retry_delay.nanos / 1e9
    if err.reason is None and code == Code.DEADLINE_EXCEEDED:
        return errors.be_error("DEADLINE_BUDGET_EXHAUSTED", message=err.message)  # P3.4
    if err.reason is None:
        err.domain = None  # unclassified (no ErrorInfo): shown to a user as INTERNAL (P4.3)
    return err


_ = code_pb2  # the canonical code numbers are the same as besdk.errors.Code
