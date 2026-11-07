from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class NeedSnapshot(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NEED_SNAPSHOT_UNSPECIFIED: _ClassVar[NeedSnapshot]
    NEED_SNAPSHOT_BEYOND_RETENTION: _ClassVar[NeedSnapshot]
    NEED_SNAPSHOT_ROLLBACK_DETECTED: _ClassVar[NeedSnapshot]
NEED_SNAPSHOT_UNSPECIFIED: NeedSnapshot
NEED_SNAPSHOT_BEYOND_RETENTION: NeedSnapshot
NEED_SNAPSHOT_ROLLBACK_DETECTED: NeedSnapshot

class Position(_message.Message):
    __slots__ = ("impl", "epoch", "seq")
    IMPL_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    impl: str
    epoch: str
    seq: int
    def __init__(self, impl: _Optional[str] = ..., epoch: _Optional[str] = ..., seq: _Optional[int] = ...) -> None: ...

class RowIdentity(_message.Message):
    __slots__ = ("echo_nonce", "record")
    class Record(_message.Message):
        __slots__ = ("record_id", "version", "digest")
        RECORD_ID_FIELD_NUMBER: _ClassVar[int]
        VERSION_FIELD_NUMBER: _ClassVar[int]
        DIGEST_FIELD_NUMBER: _ClassVar[int]
        record_id: str
        version: int
        digest: str
        def __init__(self, record_id: _Optional[str] = ..., version: _Optional[int] = ..., digest: _Optional[str] = ...) -> None: ...
    ECHO_NONCE_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    echo_nonce: str
    record: RowIdentity.Record
    def __init__(self, echo_nonce: _Optional[str] = ..., record: _Optional[_Union[RowIdentity.Record, _Mapping]] = ...) -> None: ...

class PartitionPosition(_message.Message):
    __slots__ = ("partition", "position", "row")
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    POSITION_FIELD_NUMBER: _ClassVar[int]
    ROW_FIELD_NUMBER: _ClassVar[int]
    partition: int
    position: Position
    row: RowIdentity
    def __init__(self, partition: _Optional[int] = ..., position: _Optional[_Union[Position, _Mapping]] = ..., row: _Optional[_Union[RowIdentity, _Mapping]] = ...) -> None: ...

class PullRequest(_message.Message):
    __slots__ = ("stream", "nonce", "partitions", "page_token")
    STREAM_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    stream: str
    nonce: str
    partitions: _containers.RepeatedCompositeFieldContainer[PartitionPosition]
    page_token: str
    def __init__(self, stream: _Optional[str] = ..., nonce: _Optional[str] = ..., partitions: _Optional[_Iterable[_Union[PartitionPosition, _Mapping]]] = ..., page_token: _Optional[str] = ...) -> None: ...

class Event(_message.Message):
    __slots__ = ("headers", "payload")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    headers: _containers.ScalarMap[str, str]
    payload: bytes
    def __init__(self, headers: _Optional[_Mapping[str, str]] = ..., payload: _Optional[bytes] = ...) -> None: ...

class PartitionResult(_message.Message):
    __slots__ = ("partition", "need_snapshot", "events", "echo")
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    NEED_SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    ECHO_FIELD_NUMBER: _ClassVar[int]
    partition: int
    need_snapshot: NeedSnapshot
    events: _containers.RepeatedCompositeFieldContainer[Event]
    echo: Position
    def __init__(self, partition: _Optional[int] = ..., need_snapshot: _Optional[_Union[NeedSnapshot, str]] = ..., events: _Optional[_Iterable[_Union[Event, _Mapping]]] = ..., echo: _Optional[_Union[Position, _Mapping]] = ...) -> None: ...

class Declaration(_message.Message):
    __slots__ = ("deployment", "impl", "system_id", "epoch", "primary")
    DEPLOYMENT_FIELD_NUMBER: _ClassVar[int]
    IMPL_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_ID_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_FIELD_NUMBER: _ClassVar[int]
    deployment: str
    impl: str
    system_id: str
    epoch: str
    primary: bool
    def __init__(self, deployment: _Optional[str] = ..., impl: _Optional[str] = ..., system_id: _Optional[str] = ..., epoch: _Optional[str] = ..., primary: _Optional[bool] = ...) -> None: ...

class PullResponse(_message.Message):
    __slots__ = ("source", "partitions", "next_page_token")
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    source: Declaration
    partitions: _containers.RepeatedCompositeFieldContainer[PartitionResult]
    next_page_token: str
    def __init__(self, source: _Optional[_Union[Declaration, _Mapping]] = ..., partitions: _Optional[_Iterable[_Union[PartitionResult, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...
