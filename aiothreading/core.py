# Copyright 2022 Amy Reese
# Licensed under the MIT license
# 2024 Modified by Vizonex

import asyncio
import logging
import threading
from typing import Any, Callable, Dict, Optional, Sequence

from aiologic import Event
from aiologic.lowlevel import Flag

from .types import R, Unit, PREMATURE_STOP, PrematureStopException


log = logging.getLogger(__name__)


# NOTE: Were not using multiprocessing however it's a good idea to
# have a Context to prevent variables from bleeding out
# context = Context()


async def not_implemented(*args: Any, **kwargs: Any) -> None:
    """Default function to call when none given."""
    raise NotImplementedError()


# def get_context() -> Context:
#     """Get the current active global context."""
#     global context
#     return context


class Thread:
    """Execute a coroutine on a spreate thread"""

    def __init__(
        self,
        group: None = None,
        target: Callable = None,
        name: str = None,
        args: Sequence[Any] = None,
        kwargs: Dict[str, Any] = None,
        *,
        daemon: bool = None,
        initializer: Optional[Callable] = None,
        initargs: Sequence[Any] = (),
        loop_initializer: Optional[Callable] = None,
        thread_target: Optional[Callable] = None,
        stop_event: Flag = Flag(),
    ) -> None:
        # From aiomultiprocess
        if target is not None and not asyncio.iscoroutinefunction(target):
            raise ValueError("target must be coroutine function")

        if initializer is not None and asyncio.iscoroutinefunction(initializer):
            raise ValueError("initializer must be synchronous function")

        if loop_initializer is not None and asyncio.iscoroutinefunction(
            loop_initializer
        ):
            raise ValueError("loop_initializer must be synchronous function")

        self.unit = Unit(
            target=target or not_implemented,
            args=args or (),
            kwargs=kwargs or {},
            initializer=initializer,
            initargs=initargs,
            loop_initializer=loop_initializer,
            stop_event=stop_event,
            # The Complete Event is an internal for checking that the thread exited...
            complete_event=Event(),
        )
        self.aio_thread = threading.Thread(
            group=group,
            target=thread_target or Thread.run_async,
            args=(self.unit,),
            name=name,
            daemon=daemon,
        )

    def __await__(self) -> Any:
        """Enable awaiting of the thread result by chaining to `start()` & `join()`."""
        if not self.is_alive():
            self.start()

        return self.join().__await__()

    def start(self) -> None:
        """Start the child process."""
        return self.aio_thread.start()

    async def join(self, timeout: Optional[int] = None) -> None:
        """Wait for the process to finish execution without blocking the main thread."""
        if not self.is_alive():
            raise ValueError("must start thread before joining it")

        if timeout is not None:
            return await asyncio.wait_for(self.unit.complete_event, timeout)

        await self.unit.complete_event

    # TODO: in 0.1.4 Turn Return Value into Union[R | THREAD_STOPPED_PREMATURELY_FLAG]
    # Since there's a chance that if we stop it returns with nothing...

    @staticmethod
    def run_async(unit: Unit) -> R:
        """Initialize the child thread and event loop, then execute the coroutine."""
        try:
            if unit.loop_initializer is None:
                loop = asyncio.new_event_loop()
            else:
                loop = unit.loop_initializer()

            asyncio.set_event_loop(loop)

            if unit.initializer:
                unit.initializer(*unit.initargs)

            task = loop.create_task(unit.target(*unit.args, **unit.kwargs))
            unit.stop_event.set((loop, task))
            return loop.run_until_complete(task)


        except Exception as e:
            # TODO: Better Unittests & Exception Suppressing in future update? Example might be RuntimeError after Cancellation has began...
            log.exception(f"aio thread {threading.get_ident()} failed")
            # Shutdown the loop if there was indeed failure...
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
            finally:
                pass
            raise e

        finally:
            # if we were using the "join()" method or the
            # __await__ protocol make sure that we release it here after returning the given value...
            # Shudown everything after so that nothing complains back to us with a RuntimeWarning
            asyncio.set_event_loop(None)
            loop.close()
            unit.complete_event.set()

    def start(self) -> None:
        """Start the child thread."""
        return self.aio_thread.start()

    @property
    def name(self):
        """Child Thread Name."""
        return self.aio_thread.name

    def is_alive(self) -> bool:
        """Is the thread running."""
        return self.aio_thread.is_alive() and (not self.unit.complete_event)

    @property
    def daemon(self) -> bool:
        """Should the thread be a daemon."""
        return self.aio_thread.daemon

    @daemon.setter
    def daemon(self, value: bool):
        """Should the thread be a daemon."""
        self.aio_thread.daemon = value

    @property
    def ident(self) -> Optional[int]:
        """Thread Identifier of the thread, or None if not started"""
        return self.aio_thread.ident

    @property
    def native_id(self) -> Optional[int]:
        """Native integral thread ID of this thread, or None if it has not been started."""
        return self.aio_thread.native_id

    def terminate(self):
        """Terminates the thread from running"""
        loop, task = self.unit.stop_event.get()
        # Sometimes the eventloop is closed so check to see that it's still open first...
        if not loop.is_closed():
            # Override with PREMATURE_STOP Sentient Value so that the main task 
            # will Cancel and Worker subclass throws the right error.
            loop.call_soon_threadsafe(task.set_result, PREMATURE_STOP)
            
        
class Worker(Thread):
    # TODO: fix __init__ and all arguments to it.
    def __init__(self, *args, raise_if_stopped: bool = True, **kwargs) -> None:
        super().__init__(*args, thread_target=Worker.run_async, **kwargs)
        self.unit.namespace.result = None
        self.unit.namespace.raise_if_stopped = raise_if_stopped

    @staticmethod
    def run_async(unit: Unit) -> R:
        """Initialize the thread and event loop, then execute the coroutine."""
        try:
            result: R = Thread.run_async(unit)

            unit.namespace.result = result

            return result

        except BaseException as e:
            unit.namespace.result = e
            raise

    async def join(self, timeout: int = None) -> Any:
        """Wait for the worker to finish, and return the final result."""
        await super().join(timeout)
        return self.result

    @property
    def result(self) -> R:
        """Easy access to the resulting value from the coroutine."""
        # NOTE: ValueError Might be considered redundant since we now use a sentient value.
        if self.unit.namespace.result is None:
            raise ValueError("coroutine not completed")
        elif (
            self.unit.namespace.raise_if_stopped
            and self.unit.namespace.result is PREMATURE_STOP
        ):
            raise PrematureStopException("Thread was stopped prematurely...")
        return self.unit.namespace.result
