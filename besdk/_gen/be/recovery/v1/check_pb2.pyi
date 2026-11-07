from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CheckRequest(_message.Message):
    __slots__ = ("recovery",)
    RECOVERY_FIELD_NUMBER: _ClassVar[int]
    recovery: str
    def __init__(self, recovery: _Optional[str] = ...) -> None: ...

class Difference(_message.Message):
    __slots__ = ("object_type", "object_id", "detail")
    OBJECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    object_type: str
    object_id: str
    detail: str
    def __init__(self, object_type: _Optional[str] = ..., object_id: _Optional[str] = ..., detail: _Optional[str] = ...) -> None: ...

class CheckResult(_message.Message):
    __slots__ = ("check", "basis", "outcome", "differences", "disposal")
    CHECK_FIELD_NUMBER: _ClassVar[int]
    BASIS_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    DIFFERENCES_FIELD_NUMBER: _ClassVar[int]
    DISPOSAL_FIELD_NUMBER: _ClassVar[int]
    check: str
    basis: str
    outcome: str
    differences: _containers.RepeatedCompositeFieldContainer[Difference]
    disposal: str
    def __init__(self, check: _Optional[str] = ..., basis: _Optional[str] = ..., outcome: _Optional[str] = ..., differences: _Optional[_Iterable[_Union[Difference, _Mapping]]] = ..., disposal: _Optional[str] = ...) -> None: ...

class CheckResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[CheckResult]
    def __init__(self, results: _Optional[_Iterable[_Union[CheckResult, _Mapping]]] = ...) -> None: ...

class ObjectRef(_message.Message):
    __slots__ = ("type", "id", "version")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    type: str
    id: str
    version: str
    def __init__(self, type: _Optional[str] = ..., id: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class PresentRequest(_message.Message):
    __slots__ = ("recovery", "objects")
    RECOVERY_FIELD_NUMBER: _ClassVar[int]
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    recovery: str
    objects: _containers.RepeatedCompositeFieldContainer[ObjectRef]
    def __init__(self, recovery: _Optional[str] = ..., objects: _Optional[_Iterable[_Union[ObjectRef, _Mapping]]] = ...) -> None: ...

class PresentAnswer(_message.Message):
    __slots__ = ("object", "answer", "version")
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    ANSWER_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    object: ObjectRef
    answer: str
    version: str
    def __init__(self, object: _Optional[_Union[ObjectRef, _Mapping]] = ..., answer: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class PresentResponse(_message.Message):
    __slots__ = ("answers",)
    ANSWERS_FIELD_NUMBER: _ClassVar[int]
    answers: _containers.RepeatedCompositeFieldContainer[PresentAnswer]
    def __init__(self, answers: _Optional[_Iterable[_Union[PresentAnswer, _Mapping]]] = ...) -> None: ...

class Series(_message.Message):
    __slots__ = ("legal_entity", "code", "period", "max_used")
    LEGAL_ENTITY_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    PERIOD_FIELD_NUMBER: _ClassVar[int]
    MAX_USED_FIELD_NUMBER: _ClassVar[int]
    legal_entity: str
    code: str
    period: str
    max_used: int
    def __init__(self, legal_entity: _Optional[str] = ..., code: _Optional[str] = ..., period: _Optional[str] = ..., max_used: _Optional[int] = ...) -> None: ...

class NumbersRequest(_message.Message):
    __slots__ = ("recovery",)
    RECOVERY_FIELD_NUMBER: _ClassVar[int]
    recovery: str
    def __init__(self, recovery: _Optional[str] = ...) -> None: ...

class NumbersResponse(_message.Message):
    __slots__ = ("series",)
    SERIES_FIELD_NUMBER: _ClassVar[int]
    series: _containers.RepeatedCompositeFieldContainer[Series]
    def __init__(self, series: _Optional[_Iterable[_Union[Series, _Mapping]]] = ...) -> None: ...

class AdvanceNumbersRequest(_message.Message):
    __slots__ = ("recovery", "series")
    RECOVERY_FIELD_NUMBER: _ClassVar[int]
    SERIES_FIELD_NUMBER: _ClassVar[int]
    recovery: str
    series: _containers.RepeatedCompositeFieldContainer[Series]
    def __init__(self, recovery: _Optional[str] = ..., series: _Optional[_Iterable[_Union[Series, _Mapping]]] = ...) -> None: ...

class AdvanceNumbersResponse(_message.Message):
    __slots__ = ("series",)
    SERIES_FIELD_NUMBER: _ClassVar[int]
    series: _containers.RepeatedCompositeFieldContainer[Series]
    def __init__(self, series: _Optional[_Iterable[_Union[Series, _Mapping]]] = ...) -> None: ...
