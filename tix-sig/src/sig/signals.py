"""
Minimal Python signal handlers and reversible process-global ownership.
"""

from __future__ import annotations

import os
import signal
import threading
from _thread import RLock
from collections import deque
from collections.abc import Callable, Iterable
from types import FrameType, TracebackType
from typing import Any

from ._runtime import ProcessGuard

_owners: dict[int, SignalAdapter] = {}
_fork_registered: bool = False


def _after_fork() -> None:
    # Runs only in the child, before application threads restart. Never acquire
    # inherited locks. Restore only handlers still owned by these adapters.
    """
    Restore owned signal handlers in a child without acquiring parent locks.
    """
    number: int
    adapter: SignalAdapter
    for number, adapter in tuple(_owners.items()):
        if signal.getsignal(number) is adapter._handler:
            signal.signal(number, adapter._previous[number])
    _owners.clear()


class SignalAdapter:
    """
    Install in the main thread; handlers only enqueue, poll invokes the receiver.

    Installation may replace a framework's handlers, so applications must choose one
    signal owner. This adapter never changes the process wakeup fd or event-loop policy.
    """

    def __init__(
        self: SignalAdapter,
        receiver: Callable[[str], object],
        signals: Iterable[int] = (signal.SIGINT, signal.SIGTERM),
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            receiver: Callback receiving deferred signal names outside signal context.
            signals: Catchable signal numbers to install and later restore.
        """
        self._guard: ProcessGuard = ProcessGuard()
        self._poll_lock: RLock = threading.RLock()
        self._receiver: Callable[[str], object] = receiver
        self._signals: tuple[int, ...] = tuple(dict.fromkeys(int(number) for number in signals))
        self._pending: deque[int] = deque()
        self._previous: dict[int, Any] = {}
        self._installed: bool = False
        # A stable bound method object allows identity-based ownership checks.
        self._handler: Callable[[int, FrameType | None], None] = self._receive

    @staticmethod
    def _main_thread() -> None:
        """
        Require signal registration changes to run on the main thread.
        """
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("Signals must be installed/restored in the main thread")

    def _receive(
        self: SignalAdapter,
        number: int,
        frame: FrameType | None,
    ) -> None:
        # Two notifications preserve the first cause and escalation without
        # allowing an unbounded signal storm to consume memory.
        """
        Accept a notification for deferred or buffered delivery.

        Args:
            number: Operating-system signal number.
            frame: Interrupted stack frame, when supplied by the interpreter.
        """
        if self._guard.owned and len(self._pending) < 2:
            self._pending.append(number)

    def install(
        self: SignalAdapter,
    ) -> SignalAdapter:
        """
        Install requested signal handlers and roll back partial registration.
        """
        global _fork_registered
        self._guard.check()
        self._main_thread()
        if hasattr(os, "register_at_fork") and not _fork_registered:
            os.register_at_fork(after_in_child=_after_fork)
            _fork_registered = True
        if self._installed:
            return self
        if any(number in _owners for number in self._signals):
            raise RuntimeError("A requested signal is already owned by another SignalAdapter")
        try:
            number: int
            for number in self._signals:
                self._previous[number] = signal.signal(number, self._handler)
                _owners[number] = self
        except BaseException:
            self.close()
            raise
        self._installed = True
        return self

    def poll(
        self: SignalAdapter,
    ) -> int:
        """
        Drain signals in ordinary code; return the number of delivered requests.
        """
        self._guard.check()
        with self._poll_lock:
            count: int = 0
            # Bound each drain so continuous signals cannot starve cleanup.
            _: int
            for _ in range(2):
                try:
                    number: int = self._pending.popleft()
                except IndexError:
                    break
                self._receiver(signal.Signals(number).name)
                count += 1
            return count

    def close(
        self: SignalAdapter,
    ) -> None:
        """
        Restore handlers, then deliver pending requests before releasing the adapter.
        """
        if not self._guard.owned:
            return
        self._main_thread()
        number: int
        previous: Any
        for number, previous in self._previous.items():
            if signal.getsignal(number) is self._handler:
                signal.signal(number, previous)
            if _owners.get(number) is self:
                del _owners[number]
        self._previous.clear()
        self._installed = False
        self.poll()

    def __enter__(
        self: SignalAdapter,
    ) -> SignalAdapter:
        """
        Enter the managed scope and return this instance.
        """
        return self.install()

    def __exit__(
        self: SignalAdapter,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """
        Release the resources owned by this managed scope.

        Args:
            exc_type: Exception type leaving the managed scope, if any.
            exc: Exception leaving the managed scope, if any.
            tb: Traceback associated with the exception, if any.
        """
        self.close()
