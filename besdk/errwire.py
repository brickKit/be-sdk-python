"""The error object on the wire (P3.1, P3.2): google.rpc.Status on the system surface (one ErrorInfo, a BadRequest
for field errors, a RetryInfo when to retry), problem+json on the user surface; and reading both back."""
from __future__ import annotations

import json

import grpc
from google.protobuf import any_pb2, duration_pb2
from google.rpc import code_pb2, error_details_pb2, status_pb2

from besdk import errs

_GRPC = {c: getattr(grpc.StatusCode, "CANCELLED" if c == errs.Code.CANCELLED else c.value) for c in errs.Code}


def grpc_code(code: str) -> grpc.StatusCode:
    return _GRPC.get(code, grpc.StatusCode.UNKNOWN)  # type: ignore[call-overload]


def code_of(status: grpc.StatusCode) -> str:
    if status == grpc.StatusCode.OK:
        return "OK"
    return next((c.value for c, s in _GRPC.items() if s == status), errs.Code.UNKNOWN.value)


def to_status(e: errs.Error) -> status_pb2.Status:
    """The system-surface rendering; INTERNAL carries nothing but the generic detail (P3.4)."""
    p = e.problem("")
    code = grpc.StatusCode.INTERNAL if p["reason"] == "INTERNAL" else grpc_code(e.code)
    details = [error_details_pb2.ErrorInfo(reason=p["reason"], domain=p["domain"], metadata=p.get("metadata", {}))]
    if p.get("violations"):
        details.append(error_details_pb2.BadRequest(field_violations=[
            error_details_pb2.BadRequest.FieldViolation(field=v["field"], reason=v["reason"], description=v["description"])
            for v in p["violations"]]))
    if e.retry_after > 0 and p["reason"] != "INTERNAL":
        d = duration_pb2.Duration()
        d.FromNanoseconds(int(e.retry_after * 1e9))
        details.append(error_details_pb2.RetryInfo(retry_delay=d))
    packed = []
    for m in details:
        a = any_pb2.Any()
        a.Pack(m)
        packed.append(a)
    return status_pb2.Status(code=code.value[0], message=p["detail"], details=packed)


STATUS_DETAILS_KEY = "grpc-status-details-bin"


def from_grpc(code: grpc.StatusCode, message: str, trailing: tuple | None) -> tuple[errs.Error | None, str]:
    """A system-surface error read back: (error, code); error is None without an ErrorInfo (a transport failure or a
    server outside the protocol)."""
    proto_code = code_of(code)
    raw = next((v for k, v in (trailing or ()) if k == STATUS_DETAILS_KEY), None)
    if raw is None:
        return None, proto_code
    st = status_pb2.Status.FromString(raw)
    e: errs.Error | None = None
    for d in st.details:
        if d.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
            info = error_details_pb2.ErrorInfo()
            d.Unpack(info)
            entry = errs.PLATFORM.get(info.reason) if info.domain == errs.PLATFORM_DOMAIN else None
            e = errs.Error(info.reason, info.domain, errs.Code(proto_code), entry.status if entry else errs.http_status(proto_code),
                           entry.title if entry else "", st.message or message, dict(info.metadata))
    if e is None:
        return None, proto_code
    for d in st.details:
        if d.Is(error_details_pb2.BadRequest.DESCRIPTOR):
            br = error_details_pb2.BadRequest()
            d.Unpack(br)
            e.violations += [errs.Violation(v.field, v.reason, v.description) for v in br.field_violations]
        elif d.Is(error_details_pb2.RetryInfo.DESCRIPTOR):
            ri = error_details_pb2.RetryInfo()
            d.Unpack(ri)
            e.retry_after = ri.retry_delay.ToNanoseconds() / 1e9
    return e, proto_code


def code_of_status(status: int) -> str:
    """The code of an HTTP status without a protocol body; only what decides retries and breaking is told apart."""
    if status < 400:
        return "OK"
    if status in (502, 503):
        return errs.Code.UNAVAILABLE.value
    if status == 504:
        return errs.Code.DEADLINE_EXCEEDED.value
    if status == 499:
        return errs.Code.CANCELLED.value
    if status >= 500:
        return errs.Code.INTERNAL.value
    for c in (errs.Code.INVALID_ARGUMENT, errs.Code.UNAUTHENTICATED, errs.Code.PERMISSION_DENIED, errs.Code.NOT_FOUND,
              errs.Code.ABORTED, errs.Code.RESOURCE_EXHAUSTED):
        if errs.http_status(c) == status:
            return c.value
    return errs.Code.FAILED_PRECONDITION.value


def from_http(status: int, content_type: str, retry_after: str, body: bytes) -> tuple[errs.Error | None, str]:
    """A user-surface error response read back: (error, code); error is None unless the body is problem+json with a
    reason and a domain (a proxy's page, a server outside the protocol)."""
    if content_type.split(";")[0].strip() != "application/problem+json":
        return None, code_of_status(status)
    try:
        p = json.loads(body)
    except ValueError:
        return None, code_of_status(status)
    if not isinstance(p, dict) or not p.get("reason") or not p.get("domain"):
        return None, code_of_status(status)
    entry = errs.PLATFORM.get(p["reason"]) if p["domain"] == errs.PLATFORM_DOMAIN else None
    code = entry.code.value if entry else code_of_status(status)
    e = errs.Error(p["reason"], p["domain"], errs.Code(code), status, p.get("title", ""), p.get("detail", ""),
                   dict(p.get("metadata") or {}), [errs.Violation(v.get("field", ""), v.get("reason", ""),
                                                                  v.get("description", "")) for v in p.get("violations") or []])
    if retry_after.isdigit() and int(retry_after) > 0:
        e.retry_after = float(retry_after)
    return e, code


_ = code_pb2  # the google.rpc code numbers equal grpc.StatusCode values
