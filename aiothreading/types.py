# Copied from aiomultiprocess with a few modifications

import threading
from asyncio import BaseEventLoop
from contextvars import Context
from queue import Queue
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
    Union
)
import sys 

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


class Namespace:
    def __init__(self) -> None:
        self.result = None


class Unit(NamedTuple):
    """Container for what to call on the thread."""

    target: Callable
    args: Sequence[Any]
    kwargs: Dict[str, Any]
    namespace: Optional[Namespace] = Namespace()
    initializer: Optional[Callable] = None
    initargs: Sequence[Any] = ()
    loop_initializer: Optional[LoopInitializer] = None
    runner: Optional[Callable] = None


class ProxyException(Exception):
    pass
