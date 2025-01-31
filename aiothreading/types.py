# Copied from aiomultiprocess with many modifications

from asyncio import BaseEventLoop
import enum
from typing import (
    Any,
    Callable,
    Dict,
    NamedTuple,
    NewType,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    Awaitable,
)
import sys

from aiologic import Event

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec, Concatenate
else:
    from typing import ParamSpec, Concatenate

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self


T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")
CallableOrMethod = Union[Callable[Concatenate[Self, P], T], Callable[P, T]]


TaskID = NewType("TaskID", int)
QueueID = NewType("QueueID", int)

TracebackStr = str

LoopInitializer = Callable[..., BaseEventLoop]
PoolTask = Optional[Tuple[TaskID, Callable[..., R], Sequence[T], Dict[str, T]]]
PoolResult = Tuple[TaskID, Optional[R], Optional[TracebackStr]]


# senitent ENUM FOR Worker, helps to raise PrematrueStopError when the a Worker was Stopped Prematurely
PREMATURE_STOP = enum.IntEnum(
    "_THREAD_STOPPED", "PREMATURE_STOP", start=0
).PREMATURE_STOP


class Namespace:
    def __init__(self) -> None:
        self.result = None
        self.raise_if_stopped = False


class Unit(NamedTuple):
    """Container for what to call on the thread."""

    target: Callable[..., Awaitable[R]]
    args: Sequence[Any]
    kwargs: Dict[str, Any]
    namespace: Optional[Namespace] = Namespace()
    initializer: Optional[Callable] = None
    initargs: Sequence[Any] = ()
    loop_initializer: Optional[LoopInitializer] = None
    runner: Optional[Callable] = None
    stop_event: Event = Event()
    complete_event: Event = Event()


class ProxyException(Exception):
    pass


class PrematureStopException(Exception):
    """Raised when a `Worker` Stopped Mid-Execution"""
    pass
