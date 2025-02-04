# Copyright 2022 Amy Reese
# Licensed under the MIT license
# 2024 Modified by Vizonex

import asyncio
import logging
import os
import traceback

from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from .core import Thread 
from .scheduler import Scheduler
from .types import (
    LoopInitializer,
    PoolTask,
    ProxyException,
    QueueID,
    R,
    T,
    TaskID,
    TracebackStr,
)
from .utils import deprecated_param

from aiologic import SimpleQueue, Queue

MAX_TASKS_PER_CHILD = 0  # number of tasks to execute before recycling a child process
CHILD_CONCURRENCY = 16  # number of tasks to execute simultaneously per child process
_T = TypeVar("_T")

log = logging.getLogger(__name__)


class ThreadPoolWorker(Thread):
    """Individual worker thread for the async pool."""

    @deprecated_param(
        ("ttl"),
        version="0.1.3",
        reason="Tasks To Live (TTL) will be removed in 0.1.4",
    )
    def __init__(
        self,
        tx: Union[SimpleQueue, Queue],
        rx: Union[SimpleQueue, Queue],
        ttl: int = MAX_TASKS_PER_CHILD,
        concurrency: int = CHILD_CONCURRENCY,
        *,
        initializer: Optional[Callable] = None,
        initargs: Sequence[Any] = (),
        loop_initializer: Optional[LoopInitializer] = None,
        exception_handler: Optional[Callable[[BaseException], None]] = None,
    ) -> None:
        super().__init__(
            target=self.run,
            initializer=initializer,
            initargs=initargs,
            loop_initializer=loop_initializer,
        )
        self.concurrency = max(1, concurrency)
        self.exception_handler = exception_handler
        self.ttl = max(0, ttl)
        self.tx = tx
        self.rx = rx

    async def run(self) -> None:
        """Pick up work, execute work, return results, rinse, repeat."""
        pending: Dict[asyncio.Future, TaskID] = {}
        completed = 0

        # Changes Were Suggested by x42005e1f
        # SEE: https://github.com/Vizonex/aiothreading/issues/1#issuecomment-2623569854

        def _on_completed(f: asyncio.Future):
            nonlocal completed
            nonlocal pending

            tid = pending.pop(f)

            result = None
            tb = None
            try:
                result = f.result()
            except BaseException as e:
                if self.exception_handler is not None:
                    self.exception_handler(e)

                tb = traceback.format_exc()

            self.rx.green_put((tid, result, tb))
            completed += 1

            # Were gonna remove this section in 0.1.4
            if completed == self.ttl:
                self.tx.put(None)

        while True:
            if len(pending) < self.concurrency:
                task_info = await self.tx.async_get()

                if task_info is None:
                    break

                task_id, func, args, kwargs = task_info

                future = asyncio.ensure_future(func(*args, **kwargs))
                future.add_done_callback(_on_completed)

                pending[future] = task_id
            else:
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

        if pending:
            await asyncio.wait(pending)


class ThreadPoolResult(Awaitable[Sequence[_T]], AsyncIterable[_T]):
    """
    Asynchronous proxy for map/starmap results. Can be awaited or used with `async for`.
    """

    def __init__(self, pool: "ThreadPool", task_ids: Sequence[TaskID]):
        self.pool = pool
        self.task_ids = task_ids

    def __await__(self) -> Generator[Any, None, Sequence[_T]]:
        """Wait for all results and return them as a sequence"""
        return self.results().__await__()

    async def results(self) -> Sequence[_T]:
        """Wait for all results and return them as a sequence"""
        return await self.pool.results(self.task_ids)

    def __aiter__(self) -> AsyncIterator[_T]:
        """Return results one-by-one as they are ready"""
        return self.results_generator()

    async def results_generator(self) -> AsyncIterator[_T]:
        """Return results one-by-one as they are ready"""
        for task_id in self.task_ids:
            yield (await self.pool.results([task_id]))[0]

    async def first_completed(self) -> AsyncIterator[_T]:
        """Return results one-by-one as tasks are being finished rather than by order,
        This saves the need for making a list by default and makes an asynchornous iterator for
        all the "first finishing" objects making everything smoother and more instant.
        ::

            async def coro(i):
                # Use your imagination a little...
                ...

            async with ThreadPool() as pool:
                async for finished in pool.map(coro, [1, 2, 3]).first_completed():
                    ...

        """
        # TODO: Move First_completed method inside of the ThreadPool Class Object itself rather than in here...
        task_ids = list(set(self.task_ids))
        while task_ids:
            # Pop the longest running task from the stack first
            tid = task_ids.pop(0)

            __result: Optional[Tuple[_T, Optional[TracebackStr]]] = (
                self.pool._results.pop(tid, None)
            )
            if __result:
                value, tb = __result
                if tb:
                    raise ProxyException(tb)
                else:
                    yield value
            else:
                # Place back on the stack and
                # visit other things on the eventloop
                task_ids.append(tid)
                await asyncio.sleep(0.005)


# NOTE: Not very many things have changed from aiomultiprocess's
# Pool Class Such as the removal of terminating since threads can't terminate
# Pool was also renamed to ThreadPool so aiomultiprocess doesn't overlap itself...


class ThreadPool:
    """Execute coroutines on a pool of threads."""

    @deprecated_param(
        deprecated_args=["scheduler", "maxtasksperchild"],
        version="0.1.3",
        reason="Removed for Performance Optimizations, Sheduled for deletion in 0.1.5",
    )
    def __init__(
        self,
        threads: Optional[int] = None,
        initializer: Callable[..., None] = None,
        initargs: Sequence[Any] = (),
        maxtasksperchild: int = MAX_TASKS_PER_CHILD,  # Sheduled for removal in soon as a performance optimization
        childconcurrency: int = CHILD_CONCURRENCY,
        queuecount: Optional[int] = None,  # queuecount is not used anymore
        scheduler: Optional[
            Scheduler
        ] = None,  # Scheduler is now Deprecated and no longer in use anymore
        loop_initializer: Optional[LoopInitializer] = None,
        exception_handler: Optional[Callable[[BaseException], None]] = None,
    ):
        # From concurrent.futures.ThreadPoolExecutor
        self.thread_count = min(32, threads or ((os.cpu_count() or 1) + 4))

        self.initializer = initializer
        self.initargs = initargs
        self.loop_initializer = loop_initializer
        self.maxtasksperchild = max(0, maxtasksperchild)
        self.childconcurrency = max(1, childconcurrency)
        self.exception_handler = exception_handler

        # NOTE: Renamed processes to threads since were dealing with threads - Vizonex

        # Were going to use a list instead of a dicitonary for initalization
        # This is more or less an optimization
        self.threads: list[Thread] = []

        # Queue count is important to keep because of chunking
        # Lets say we have a textfile that is 10 GB (Gigabytes)
        # We want to go through all of them but the ram in our
        # computer is minimal it could cause a memory-error

        # So holding onto queue count is important in that case and shall not be removed.

        self.queue_count = queuecount

        if self.queue_count is not None:
            if self.queue_count > self.thread_count:
                raise ValueError("queue count must be <= thread count")

            self.rx = Queue(self.queue_count)
            self.tx = Queue(self.queue_count)
        else:
            self.rx = SimpleQueue()
            self.tx = SimpleQueue()

        self.running = True
        self.last_id = 0
        self._results: Dict[TaskID, Tuple[Any, Optional[TracebackStr]]] = {}

        self.init()
        self._loop = asyncio.ensure_future(self.loop())

    async def __aenter__(self) -> "ThreadPool":
        """Enable `async with ThreadPool() as pool` usage."""
        return self

    async def __aexit__(self, *args) -> None:
        """Automatically terminate the pool when falling out of scope."""
        self.terminate()
        await self.join()

    def init(self) -> None:
        """
        Create the initial mapping of threads and queues.

        :meta private:
        """

        while len(self.threads) != self.thread_count:
            self.threads.append(self.create_worker())

    async def loop(self) -> None:
        """
        Maintain the pool of workers while open.

        :meta private:
        """
        while self.threads or self.running:
            task_info = await self.rx.async_get()

            if task_info is None:
                continue

            self.finish_work(*task_info)

    def create_worker(self) -> Thread:
        """
        Create a worker thread attached to the given transmit and receive queues.

        :meta private:
        """
        thread = ThreadPoolWorker(
            self.tx,
            self.rx,
            self.maxtasksperchild,
            self.childconcurrency,
            initializer=self.initializer,
            initargs=self.initargs,
            loop_initializer=self.loop_initializer,
            exception_handler=self.exception_handler,
        )
        thread.start()
        return thread

    def queue_work(
        self,
        func: Callable[..., Awaitable[R]],
        args: Sequence[Any],
        kwargs: Dict[str, Any],
    ) -> TaskID:
        """
        Add a new work item to the outgoing queue.

        :meta private:
        """
        self.last_id += 1
        task_id = TaskID(self.last_id)
        self.tx.put_nowait((task_id, func, args, kwargs))
        return task_id

    def finish_work(
        self, task_id: TaskID, value: Any, tb: Optional[TracebackStr]
    ) -> None:
        """
        Mark work items as completed.

        :meta private:
        """
        self._results[task_id] = value, tb

    async def results(self, tids: Sequence[TaskID]) -> Sequence[R]:
        """
        Wait for all tasks to complete, and return results, preserving order.

        :meta private:
        """
        pending = set(tids)
        ready: Dict[TaskID, R] = {}

        while pending:
            for tid in pending.copy():
                if tid in self._results:
                    result, tb = self._results.pop(tid)
                    if tb is not None:
                        raise ProxyException(tb)
                    ready[tid] = result
                    pending.remove(tid)

            await asyncio.sleep(0.005)

        return [ready[tid] for tid in tids]

    async def apply(
        self,
        func: Callable[..., Awaitable[R]],
        args: Sequence[Any] = None,
        kwds: Dict[str, Any] = None,
    ) -> R:
        """Run a single coroutine on the pool."""

        if not self.running:
            raise RuntimeError("pool is closed")

        args = args or ()
        kwds = kwds or {}

        tid = self.queue_work(func, args, kwds)
        results: Sequence[R] = await self.results([tid])
        return results[0]

    def map(
        self,
        func: Callable[[T], Awaitable[R]],
        iterable: Sequence[T],
        # TODO: Let's fulfill Amy's Request by 0.1.7 for our library to implement chunking...
        # chunksize: int = None,  # todo: implement chunking maybe
    ) -> ThreadPoolResult[R]:
        """Run a coroutine once for each item in the iterable."""
        if not self.running:
            raise RuntimeError("pool is closed")

        tids = [self.queue_work(func, (item,), {}) for item in iterable]
        return ThreadPoolResult(self, tids)

    def starmap(
        self,
        func: Callable[..., Awaitable[R]],
        iterable: Sequence[Sequence[T]],
        # chunksize: int = None,  # todo: implement chunking maybe
    ) -> ThreadPoolResult[R]:
        """Run a coroutine once for each sequence of items in the iterable."""
        if not self.running:
            raise RuntimeError("pool is closed")

        tids = [self.queue_work(func, args, {}) for args in iterable]
        return ThreadPoolResult(self, tids)

    # TODO: Turn Close and Terminate into async functions?
    def close(self) -> None:
        """Close the pool to new visitors."""
        self.running = False

        # TODO: Better signals for stopping threads from running.
        for t in self.threads:
            if t.is_alive():
                self.tx.green_put(None)

    def terminate(self) -> None:
        """No running by the pool!"""
        if self.running:
            self.close()

        for t in self.threads:
            t.terminate()

    async def join(self) -> None:
        """Waits for the pool to be finished gracefully."""
        if self.running:
            raise RuntimeError("pool is still open")

        await self._loop
