from besdk._gen.be.source.v1 import watermark_pb2 as _watermark_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PartitionFrom(_message.Message):
    __slots__ = ("partition", "after")
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    AFTER_FIELD_NUMBER: _ClassVar[int]
    partition: int
    after: _watermark_pb2.Position
    def __init__(self, partition: _Optional[int] = ..., after: _Optional[_Union[_watermark_pb2.Position, _Mapping]] = ...) -> None: ...

class ResendRequest(_message.Message):
    __slots__ = ("stream", "request_id")
    STREAM_FIELD_NUMBER: _ClassVar[int]
    FROM_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    stream: str
    request_id: str
    def __init__(self, stream: _Optional[str] = ..., request_id: _Optional[str] = ..., **kwargs) -> None: ...

class ResendResponse(_message.Message):
    __slots__ = ("resent", "beyond_retention", "ahead")
    RESENT_FIELD_NUMBER: _ClassVar[int]
    BEYOND_RETENTION_FIELD_NUMBER: _ClassVar[int]
    AHEAD_FIELD_NUMBER: _ClassVar[int]
    resent: int
    beyond_retention: _containers.RepeatedScalarFieldContainer[int]
    ahead: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, resent: _Optional[int] = ..., beyond_retention: _Optional[_Iterable[int]] = ..., ahead: _Optional[_Iterable[int]] = ...) -> None: ...
