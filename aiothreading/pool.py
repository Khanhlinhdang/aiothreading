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
    Generic,
    overload
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

from aiologic import SimpleQueue, Queue, REvent

MAX_TASKS_PER_CHILD = 0  # number of tasks to execute before recycling a child process
CHILD_CONCURRENCY = 16  # number of tasks to execute simultaneously per child process
_T = TypeVar("_T")

log = logging.getLogger(__name__)


# Based off concurrent.futures.thread
# This would solve multiple problems with `results`, `apply`, `loop` and `finish_work` 
class _WorkItem(Generic[T]):
    def __init__(
        self,
        future:asyncio.Future[T], 
        loop:asyncio.AbstractEventLoop, 
        fn:Callable[..., Awaitable[T]], 
        args:Tuple, 
        kwargs:Dict,

        exception_handler: Optional[Callable[[BaseException], None]]
    ):
        # _loop would be the main loop
        self._loop = loop
        self.future = future

        # This future could be returned inside of a submit function.
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.exception_handler = exception_handler
    
    async def run(self):
        try:
            result = await self.fn(*self.args, **self.kwargs)
        except Exception as exc:
            if self.exception_handler:
                self.exception_handler(exc)
            else:
                self._loop.call_soon_threadsafe(self.future.set_exception, ProxyException(traceback.format_exc()))
        else:
            self._loop.call_soon_threadsafe(self.future.set_result, result)

        # Return `self` so the future carrying the object can 
        # dereference it with delete "del"
        return self 
    

class ThreadPoolWorker(Thread):
    """Individual worker thread for the async pool."""


    @deprecated_param(
        ("ttl"),
        version="0.1.3",
        reason="Tasks To Live (TTL) will be removed in 0.1.4",
    )
    # @deprecated_param(
    #     "exception_handler",
    #     version="0.1.3",
    #     reason="Moved into _WorkItem will be removed in 0.1.4"
    # )
    def __init__(
        self,
        tx: Union[SimpleQueue[tuple[TaskID, _WorkItem]], Queue[TaskID, _WorkItem]],
        ttl: int = MAX_TASKS_PER_CHILD,
        concurrency: int = CHILD_CONCURRENCY,
        *,
        initializer: Optional[Callable] = None,
        initargs: Sequence[Any] = (),
        loop_initializer: Optional[LoopInitializer] = None,
        # Moved to _WorkItem
        exception_handler: Optional[Callable[[BaseException], None]] = None,
    ) -> None:
        super().__init__(
            target=self.run,
            initializer=initializer,
            initargs=initargs,
            loop_initializer=loop_initializer,
        )
        self.concurrency = max(1, concurrency)
        # self.exception_handler = exception_handler
        self.ttl = max(0, ttl)
        self.tx = tx


    async def run(self) -> None:
        """Pick up work, execute work, return results, rinse, repeat."""
        pending: Dict[asyncio.Future[_WorkItem], TaskID] = {}
        completed = 0

        # Changes Were Suggested by x42005e1f
        # SEE: https://github.com/Vizonex/aiothreading/issues/1#issuecomment-2623569854

        def _on_completed(f: asyncio.Future[_WorkItem]):
            nonlocal completed
            nonlocal pending

            pending.pop(f)
            
            # NOTE: (Vizonex) I put the exception handler in the _WorkItem
            item = f.result()
            del item

            completed += 1

            # # Were gonna remove this section in 0.1.4
            if completed == self.ttl:
                self.tx.put(None)

        while True:
            if len(pending) < self.concurrency:
                task_info = await self.tx.async_get()

                if task_info is None:
                    break
                
                tid , work = task_info
                future = asyncio.ensure_future(work.run())
                future.add_done_callback(_on_completed)
                pending[future] = tid
            else:
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

        if pending:
            await asyncio.wait(pending)

    


class ThreadPoolResult(Awaitable[Sequence[_T]], AsyncIterable[_T]):
    """
    Asynchronous proxy for map/starmap results. Can be awaited or used with `async for`.
    """

    def __init__(self, pool: "ThreadPool", task_iterator:AsyncIterable[_T]):
        self.pool = pool
        self.tasks = task_iterator

    def __await__(self) -> Generator[Any, None, Sequence[_T]]:
        """Wait for all results and return them as a sequence"""
        return self.results().__await__()

    async def results(self) -> Sequence[_T]:
        """Wait for all results and return them as a sequence"""
        return [t async for t in self.tasks]

    def __aiter__(self) -> AsyncIterator[_T]:
        """Return results one-by-one as they are ready"""
        return self.tasks

    async def results_generator(self) -> AsyncIterator[_T]:
        """Return results one-by-one as they are ready"""
        async for r in self.tasks:
            yield r

           


    



# NOTE: Not very many things have changed from aiomultiprocess's
# Pool Class Such as the removal of terminating since threads can't terminate
# Pool was also renamed to ThreadPool so aiomultiprocess doesn't overlap itself...


class ThreadPool:
    """Execute coroutines on a pool of threads."""

    # TODO: We will need to fix issues with pyright and mypy in the future...
    @overload
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
    ):...

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
            self.tx = Queue(self.queue_count)
        else:
            self.tx = SimpleQueue()

        self.tasks_running = 0 
        self.tasks_complete = REvent(True)

        self.running = True
        self.last_id = 0
        # self._results: Dict[TaskID, Tuple[Any, Optional[TracebackStr]]] = {}

        self.init()
        self._loop = asyncio.get_event_loop()
        # self._loop = asyncio.ensure_future(self.loop())


 

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

    def create_worker(self) -> Thread:
        """
        Create a worker thread attached to the given transmit and receive queues.

        :meta private:
        """
        thread = ThreadPoolWorker(
            self.tx,
            self.maxtasksperchild,
            self.childconcurrency,
            initializer=self.initializer,
            initargs=self.initargs,
            loop_initializer=self.loop_initializer,
            exception_handler=self.exception_handler,
        )
        thread.start()
        return thread

    def submit(
        self,
        func: Callable[..., Awaitable[R]],
        args: Sequence[Any],
        kwargs: Dict[str, Any],
    ) -> asyncio.Future[R]:
        """
        Add a new work item to the outgoing queue.
        returns a future
        
        """
        if not self.running:
            raise RuntimeError("pool is closed")
        
        def on_complete(fut):
            self.tasks_running -= 1
            if self.tasks_running <= 0:
                self.tasks_complete.set()


        self.tasks_complete.clear()
        self.tasks_running += 1

        self.last_id += 1

        task_id = TaskID(self.last_id)
        fut = self._loop.create_future()
        
        
        fut.add_done_callback()

        self.tx.put((task_id, _WorkItem(fut, self._loop, func, args, kwargs, self.exception_handler)))
        return fut

    # TODO: Deprecate apply in replacement of submit...
    async def apply(
        self,
        func: Callable[..., Awaitable[R]],
        args: Sequence[Any] = None,
        kwds: Dict[str, Any] = None,
    ) -> R:
        """Run a single coroutine on the pool."""

        if not self.running:
            raise RuntimeError("pool is closed")
        
        return await self.submit(func,  args or () , kwds or {})


    def map(
        self,
        func: Callable[[T], Awaitable[R]],
        iterable: Sequence[T],
        chunksize: Optional[int] = None
    ) -> ThreadPoolResult[R]:
        """Run a coroutine once for each item in the iterable."""
        if not self.running:
            raise RuntimeError("pool is closed")
        
        if chunksize is None or chunksize <= 1:
            chunksize = self.childconcurrency * self.thread_count

        async def chunking():
            nonlocal iterable
            running = True
            chunks = set()
            it = iter(iterable)
            while running:

                while len(chunks) < chunksize:
                    try:
                        chunks.add(self.submit(func, (next(it),), {}))
                    except StopIteration:
                        running = False
                        break

                done, _ = await asyncio.wait(chunks, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
                for d in done:
                    chunks.remove(d)
                    yield await d
            
            if chunks:
                for completed in asyncio.as_completed(chunks):
                    yield await completed
        
        return ThreadPoolResult(self, chunking())
        

    def starmap(
        self,
        func: Callable[..., Awaitable[R]],
        iterable: Sequence[Sequence[T]],
        chunksize: Optional[int] = None
    ) -> ThreadPoolResult[R]:
        """Run a coroutine once for each sequence of items in the iterable."""
        if not self.running:
            raise RuntimeError("pool is closed")
        
        if chunksize is None or chunksize <= 1:
            chunksize = self.childconcurrency * self.thread_count


        async def chunking():
            nonlocal iterable
            running = True
            chunks = set()
            it = iter(iterable)
            while running:

                while len(chunks) < chunksize:
                    try:
                        chunks.add(self.submit(func, next(it), {}))
                    except StopIteration:
                        running = False
                        break

                done, _ = await asyncio.wait(chunks, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
                for d in done:
                    chunks.remove(d)
                    yield await d
            
            if chunks:
                for completed in asyncio.as_completed(chunks):
                    yield await completed
            
        return ThreadPoolResult(self, chunking())
    
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
        
        # wait for the futures submitted to complete...
        await self.tasks_complete
        
        for t in self.threads:
            await t
        


