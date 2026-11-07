from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ReplayRequest(_message.Message):
    __slots__ = ("record", "recovery")
    RECORD_FIELD_NUMBER: _ClassVar[int]
    RECOVERY_FIELD_NUMBER: _ClassVar[int]
    record: bytes
    recovery: str
    def __init__(self, record: _Optional[bytes] = ..., recovery: _Optional[str] = ...) -> None: ...

class ReplayResponse(_message.Message):
    __slots__ = ("outcome", "detail")
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    outcome: str
    detail: str
    def __init__(self, outcome: _Optional[str] = ..., detail: _Optional[str] = ...) -> None: ...
