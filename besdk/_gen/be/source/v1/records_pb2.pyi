from besdk._gen.be.source.v1 import watermark_pb2 as _watermark_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ReadState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    READ_STATE_UNSPECIFIED: _ClassVar[ReadState]
    READ_STATE_EXISTS: _ClassVar[ReadState]
    READ_STATE_TOMBSTONE: _ClassVar[ReadState]
    READ_STATE_PURGED: _ClassVar[ReadState]
READ_STATE_UNSPECIFIED: ReadState
READ_STATE_EXISTS: ReadState
READ_STATE_TOMBSTONE: ReadState
READ_STATE_PURGED: ReadState

class PartitionHigh(_message.Message):
    __slots__ = ("partition", "seq")
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    partition: int
    seq: int
    def __init__(self, partition: _Optional[int] = ..., seq: _Optional[int] = ...) -> None: ...

class Record(_message.Message):
    __slots__ = ("record_id", "version", "payload", "claim")
    RECORD_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CLAIM_FIELD_NUMBER: _ClassVar[int]
    record_id: str
    version: int
    payload: bytes
    claim: bool
    def __init__(self, record_id: _Optional[str] = ..., version: _Optional[int] = ..., payload: _Optional[bytes] = ..., claim: _Optional[bool] = ...) -> None: ...

class ReadRequest(_message.Message):
    __slots__ = ("stream", "record_ids")
    STREAM_FIELD_NUMBER: _ClassVar[int]
    RECORD_IDS_FIELD_NUMBER: _ClassVar[int]
    stream: str
    record_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, stream: _Optional[str] = ..., record_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class ReadResult(_message.Message):
    __slots__ = ("record_id", "state", "version", "payload", "claim")
    RECORD_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CLAIM_FIELD_NUMBER: _ClassVar[int]
    record_id: str
    state: ReadState
    version: int
    payload: bytes
    claim: bool
    def __init__(self, record_id: _Optional[str] = ..., state: _Optional[_Union[ReadState, str]] = ..., version: _Optional[int] = ..., payload: _Optional[bytes] = ..., claim: _Optional[bool] = ...) -> None: ...

class ReadResponse(_message.Message):
    __slots__ = ("source", "results", "highs")
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    HIGHS_FIELD_NUMBER: _ClassVar[int]
    source: _watermark_pb2.Declaration
    results: _containers.RepeatedCompositeFieldContainer[ReadResult]
    highs: _containers.RepeatedCompositeFieldContainer[PartitionHigh]
    def __init__(self, source: _Optional[_Union[_watermark_pb2.Declaration, _Mapping]] = ..., results: _Optional[_Iterable[_Union[ReadResult, _Mapping]]] = ..., highs: _Optional[_Iterable[_Union[PartitionHigh, _Mapping]]] = ...) -> None: ...

class SnapshotRequest(_message.Message):
    __slots__ = ("stream", "page_token")
    STREAM_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    stream: str
    page_token: str
    def __init__(self, stream: _Optional[str] = ..., page_token: _Optional[str] = ...) -> None: ...

class SnapshotResponse(_message.Message):
    __slots__ = ("source", "records", "highs", "next_page_token")
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    HIGHS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    source: _watermark_pb2.Declaration
    records: _containers.RepeatedCompositeFieldContainer[Record]
    highs: _containers.RepeatedCompositeFieldContainer[PartitionHigh]
    next_page_token: str
    def __init__(self, source: _Optional[_Union[_watermark_pb2.Declaration, _Mapping]] = ..., records: _Optional[_Iterable[_Union[Record, _Mapping]]] = ..., highs: _Optional[_Iterable[_Union[PartitionHigh, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...
