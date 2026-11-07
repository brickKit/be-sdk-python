from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ExchangeRequest(_message.Message):
    __slots__ = ("session_digest", "renew_idle")
    SESSION_DIGEST_FIELD_NUMBER: _ClassVar[int]
    RENEW_IDLE_FIELD_NUMBER: _ClassVar[int]
    session_digest: str
    renew_idle: bool
    def __init__(self, session_digest: _Optional[str] = ..., renew_idle: _Optional[bool] = ...) -> None: ...

class ExchangeResponse(_message.Message):
    __slots__ = ("access_token", "expires_at_ms", "principal", "session_until_ms")
    ACCESS_TOKEN_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    SESSION_UNTIL_MS_FIELD_NUMBER: _ClassVar[int]
    access_token: str
    expires_at_ms: int
    principal: str
    session_until_ms: int
    def __init__(self, access_token: _Optional[str] = ..., expires_at_ms: _Optional[int] = ..., principal: _Optional[str] = ..., session_until_ms: _Optional[int] = ...) -> None: ...

class Cursor(_message.Message):
    __slots__ = ("impl", "epoch", "seq")
    IMPL_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    impl: str
    epoch: str
    seq: int
    def __init__(self, impl: _Optional[str] = ..., epoch: _Optional[str] = ..., seq: _Optional[int] = ...) -> None: ...

class EndedRequest(_message.Message):
    __slots__ = ("after", "limit")
    AFTER_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    after: Cursor
    limit: int
    def __init__(self, after: _Optional[_Union[Cursor, _Mapping]] = ..., limit: _Optional[int] = ...) -> None: ...

class EndedResponse(_message.Message):
    __slots__ = ("session_digests", "next", "reset")
    SESSION_DIGESTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_FIELD_NUMBER: _ClassVar[int]
    RESET_FIELD_NUMBER: _ClassVar[int]
    session_digests: _containers.RepeatedScalarFieldContainer[str]
    next: Cursor
    reset: bool
    def __init__(self, session_digests: _Optional[_Iterable[str]] = ..., next: _Optional[_Union[Cursor, _Mapping]] = ..., reset: _Optional[bool] = ...) -> None: ...
