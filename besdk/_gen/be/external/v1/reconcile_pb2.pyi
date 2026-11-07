from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ReconcileRequest(_message.Message):
    __slots__ = ("recovery", "t", "window_before_seconds", "issued", "failed_at")
    RECOVERY_FIELD_NUMBER: _ClassVar[int]
    T_FIELD_NUMBER: _ClassVar[int]
    WINDOW_BEFORE_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ISSUED_FIELD_NUMBER: _ClassVar[int]
    FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    recovery: str
    t: str
    window_before_seconds: int
    issued: _containers.RepeatedScalarFieldContainer[str]
    failed_at: str
    def __init__(self, recovery: _Optional[str] = ..., t: _Optional[str] = ..., window_before_seconds: _Optional[int] = ..., issued: _Optional[_Iterable[str]] = ..., failed_at: _Optional[str] = ...) -> None: ...

class Conclusion(_message.Message):
    __slots__ = ("reference", "command", "conclusion", "state", "resend_deadline", "detail")
    REFERENCE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    CONCLUSION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    RESEND_DEADLINE_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    reference: str
    command: str
    conclusion: str
    state: str
    resend_deadline: str
    detail: str
    def __init__(self, reference: _Optional[str] = ..., command: _Optional[str] = ..., conclusion: _Optional[str] = ..., state: _Optional[str] = ..., resend_deadline: _Optional[str] = ..., detail: _Optional[str] = ...) -> None: ...

class ReconcileResponse(_message.Message):
    __slots__ = ("conclusions",)
    CONCLUSIONS_FIELD_NUMBER: _ClassVar[int]
    conclusions: _containers.RepeatedCompositeFieldContainer[Conclusion]
    def __init__(self, conclusions: _Optional[_Iterable[_Union[Conclusion, _Mapping]]] = ...) -> None: ...
