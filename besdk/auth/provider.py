"""The calls a component makes to the authorization provider's gRPC service ``infra.authz.v2.AuthzProvider``
at ``AUTHZ_GRPC_URL`` (be-protocol P6.10, contract-infra-authz). The messages come from the contract's
descriptor set shipped with besdk, loaded into a private descriptor pool, so a component that also imports
the contract's own generated code never meets a duplicate symbol."""

from __future__ import annotations

from functools import cache
from importlib import resources
from typing import TYPE_CHECKING, Any

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from besdk import errors

if TYPE_CHECKING:
    from besdk.runtime import Runtime

SERVICE = "/infra.authz.v2.AuthzProvider/"


@cache
def _pool() -> descriptor_pool.DescriptorPool:
    data = resources.files("besdk").joinpath("_protocol/authz-provider-v2.binpb").read_bytes()
    pool = descriptor_pool.DescriptorPool()
    for f in descriptor_pb2.FileDescriptorSet.FromString(data).file:
        pool.Add(f)
    return pool


@cache
def message(name: str) -> Any:
    """The message class ``infra.authz.v2.<name>`` (e.g. ``WriteTuplesRequest``)."""
    return message_factory.GetMessageClass(_pool().FindMessageTypeByName("infra.authz.v2." + name))


def tuple_msg(rtype: str, rid: str, relation: str, subject: str, expires_at: Any = None) -> Any:
    t = message("Tuple")(object=message("ObjectRef")(type=rtype, id=rid), relation=relation, subject=subject)
    if expires_at is not None:
        t.expires_at.FromDatetime(expires_at)
    return t


_KINDS = {"user": 1, "agent": 2, "svc": 3}


def actor(sub: str, act: Any = None) -> Any:
    """The person who acts, with the token's ``act`` chain when delegated (RFC 8693 §4.1)."""
    a = message("Actor")(sub=sub, kind=1)
    if isinstance(act, dict) and act.get("sub"):
        nested = actor(act["sub"], act.get("act"))
        nested.kind = _KINDS.get(act.get("kind", ""), 0)
        a.act.CopyFrom(nested)
    return a


async def write_tuples(rt: "Runtime", *, writes: list = (), deletes: list = (), idempotency_key: str = "",
                       actor_sub: str = "", act: Any = None) -> str:
    """WriteTuples (capability ``sharing``); returns the provider's revision."""
    ch = rt.conn("infra/authz", "grpc")
    if ch is None:
        raise errors.be_error("CAPABILITY_UNAVAILABLE", {"capability": "sharing"},
                              message="AUTHZ_GRPC_URL is not configured")
    req = message("WriteTuplesRequest")(writes=list(writes), deletes=list(deletes), idempotency_key=idempotency_key,
                                        actor=actor(actor_sub, act), source=rt.id)
    call = ch.unary_unary(SERVICE + "WriteTuples", request_serializer=lambda m: m.SerializeToString(),
                          response_deserializer=message("WriteTuplesResponse").FromString)
    resp = await call(req)
    return resp.revision
