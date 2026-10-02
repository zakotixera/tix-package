"""
First-cause cancellation with thread-safe, event-driven asyncio notification.
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from _thread import LockType
from collections import deque

from ._runtime import ProcessGuard, deadline


class ShutdownRequested(
    Exception,
):
    """
    Raised at an explicit cooperative cancellation checkpoint.
    """


class CancellationToken:
    """
    Coordinate first-cause cancellation across threads, loops, and child tokens.
    """

    def __init__(
        self: CancellationToken,
    ) -> None:
        """
        Initialize the instance and its owned state.
        """
        self._guard: ProcessGuard = ProcessGuard()
        self._event: threading.Event = threading.Event()
        self._lock: LockType = threading.Lock()
        self._reason: str | None = None
        self._waiters: set[tuple[asyncio.AbstractEventLoop, asyncio.Future[str]]] = set()
        self._children: weakref.WeakSet[CancellationToken] = weakref.WeakSet()
        self._parent: CancellationToken | None = None

    @property
    def cancelled(
        self: CancellationToken,
    ) -> bool:
        """
        Return whether cancellation has already been requested.
        """
        self._guard.check()
        with self._lock:
            return self._reason is not None

    @property
    def reason(
        self: CancellationToken,
    ) -> str | None:
        """
        Return the first recorded cancellation cause, if any.
        """
        self._guard.check()
        with self._lock:
            return self._reason

    def child(
        self: CancellationToken,
    ) -> CancellationToken:
        """
        Create a linked token. Parent cancellation flows down, never upward.
        """
        self._guard.check()
        child: CancellationToken = CancellationToken()
        with self._lock:
            reason: str | None = self._reason
            if reason is None:
                self._children.add(child)
                child._parent = self
        if reason is not None:
            child.cancel(reason)
        return child

    def cancel(
        self: CancellationToken,
        reason: str = "shutdown",
    ) -> bool:
        """
        Request cancellation while preserving the first recorded cause.

        Args:
            reason: First-cause description retained when cancellation is requested.
        """
        self._guard.check()
        if not isinstance(reason, str):
            raise TypeError("reason must be a string")
        pending: deque[CancellationToken] = deque([self])
        first: bool = False
        while pending:
            token: CancellationToken = pending.popleft()
            with token._lock:
                if token._reason is not None:
                    continue
                token._reason = reason
                token._event.set()
                waiters: tuple[tuple[asyncio.AbstractEventLoop, asyncio.Future[str]], ...] = tuple(
                    token._waiters
                )
                token._waiters.clear()
                pending.extend(token._children)
                token._children.clear()
                parent: CancellationToken | None
                parent, token._parent = token._parent, None
                if token is self:
                    first = True
            if parent is not None:
                with parent._lock:
                    parent._children.discard(token)
            loop: asyncio.AbstractEventLoop
            future: asyncio.Future[str]
            notifications: dict[asyncio.AbstractEventLoop, list[asyncio.Future[str]]] = {}
            for loop, future in waiters:
                notifications.setdefault(loop, []).append(future)
            futures: list[asyncio.Future[str]]
            for loop, futures in notifications.items():
                try:
                    loop.call_soon_threadsafe(self._resolve, futures, reason)
                except RuntimeError:
                    # A stopped/closed loop owns its abandoned futures. Other loops
                    # must still receive cancellation; no foreign-thread mutation.
                    continue
        return first

    @staticmethod
    def _resolve(
        futures: list[asyncio.Future[str]],
        reason: str,
    ) -> None:
        """
        Complete a batch of live cancellation waiters on their owning event loop.

        Args:
            futures: Futures completed on their shared event loop.
            reason: First-cause description retained when cancellation is requested.
        """
        future: asyncio.Future[str]
        for future in futures:
            if not future.done():
                future.set_result(reason)

    def checkpoint(
        self: CancellationToken,
    ) -> None:
        """
        Raise at a cooperative checkpoint after cancellation is requested.
        """
        reason: str | None
        if (reason := self.reason) is not None:
            raise ShutdownRequested(reason)

    def wait(
        self: CancellationToken,
        timeout: float | None = None,
    ) -> bool:
        """
        Wait synchronously for cancellation or deadline expiry.

        Args:
            timeout: Maximum wait in seconds, or None for no deadline.
        """
        self._guard.check()
        deadline(timeout)
        return self._event.wait(timeout)

    async def wait_async(
        self: CancellationToken,
        *,
        timeout: float | None = None,
    ) -> str:
        """
        Await cancellation on any loop without polling or executor threads.

        Args:
            timeout: Maximum wait in seconds, or None for no deadline.
        """
        self._guard.check()
        deadline(timeout)
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        with self._lock:
            if self._reason is not None:
                return self._reason
            future: asyncio.Future[str] = loop.create_future()
            waiter: tuple[asyncio.AbstractEventLoop, asyncio.Future[str]] = (loop, future)
            self._waiters.add(waiter)
        try:
            async with asyncio.timeout(timeout):
                return await future
        finally:
            with self._lock:
                self._waiters.discard(waiter)
