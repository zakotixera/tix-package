"""
Phased, at-most-once cleanup independent of signal reception.
"""

from __future__ import annotations

import asyncio
import signal
import threading
import time
import weakref
from _thread import RLock
from collections.abc import Callable, Iterable
from typing import Any

from ._hooks import Hook, HookRunner, plan
from ._runtime import ProcessGuard, deadline, remaining
from .models import Phase, ShutdownContext, ShutdownReport, State
from .signals import SignalAdapter
from .token import CancellationToken


class ShutdownManager:
    """
    Request first, let application tasks stop, then explicitly run cleanup.

    Sync hooks in async shutdown run inline and must not block. Budgets are
    cooperative; neither threads nor cancellation-resistant coroutines can be killed.
    """

    def __init__(
        self: ShutdownManager,
        *,
        timeout: float | None = None,
        poll_interval: float = 0.02,
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            timeout: Maximum wait in seconds, or None for no deadline.
            poll_interval: Seconds between polls for deferred operating-system signals.
        """
        deadline(timeout)
        deadline(poll_interval)
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        self._guard: ProcessGuard = ProcessGuard()
        self.token: CancellationToken = CancellationToken()
        self._completion: CancellationToken = CancellationToken()
        self._timeout: float | None = timeout
        self._poll_interval: float = poll_interval
        self._lock: RLock = threading.RLock()
        self._state: State = State.ACTIVE
        self._escalated: bool = False
        self._hooks: dict[str, Hook] = {}
        self._sequence: int = 0
        self._report: ShutdownReport | None = None
        self._runner_thread: int | None = None
        self._runner_task: asyncio.Task[Any] | None = None
        self._adapters: weakref.WeakSet[SignalAdapter] = weakref.WeakSet()

    @property
    def state(
        self: ShutdownManager,
    ) -> State:
        """
        Drain pending signals and return the shutdown lifecycle state.
        """
        self.poll()
        self._guard.check()
        with self._lock:
            return self._state

    @property
    def escalated(
        self: ShutdownManager,
    ) -> bool:
        """
        Return whether a repeated shutdown request asked for escalation.
        """
        self.poll()
        self._guard.check()
        with self._lock:
            return self._escalated

    def _reason(
        self: ShutdownManager,
    ) -> str:
        """
        Return the first shutdown cause or the default description.
        """
        reason: str | None = self.token.reason
        return reason if reason is not None else "shutdown"

    def request(
        self: ShutdownManager,
        reason: str = "shutdown",
    ) -> bool:
        """
        Request cooperative shutdown and track repeated requests.

        Args:
            reason: First-cause description retained when cancellation is requested.
        """
        self._guard.check()
        with self._lock:
            if self._state is State.CLOSED:
                return False
            first: bool = self.token.cancel(reason)
            if first:
                if self._state is State.ACTIVE:
                    self._state = State.REQUESTED
            else:
                self._escalated = True
            return first

    def signals(
        self: ShutdownManager,
        signals: Iterable[int] = (signal.SIGINT, signal.SIGTERM),
    ) -> SignalAdapter:
        """
        Create an adapter; use its context manager to install and restore handlers.

        Args:
            signals: Catchable signal numbers to install and later restore.
        """
        self._guard.check()
        with self._lock:
            if self._state is State.CLOSED:
                raise RuntimeError("ShutdownManager is closed")
            adapter: SignalAdapter = SignalAdapter(self.request, signals)
            self._adapters.add(adapter)
            return adapter

    def poll(
        self: ShutdownManager,
    ) -> int:
        """
        Drain pending notifications from this manager's signal adapters.
        """
        self._guard.check()
        with self._lock:
            adapters: tuple[SignalAdapter, ...] = tuple(self._adapters)
        return sum(adapter.poll() for adapter in adapters)

    def wait(
        self: ShutdownManager,
        timeout: float | None = None,
    ) -> bool:
        """
        Wait synchronously for cancellation or deadline expiry.

        Args:
            timeout: Maximum wait in seconds, or None for no deadline.
        """
        end: float | None = deadline(timeout)
        while True:
            self.poll()
            if self.token.cancelled:
                return True
            budget: float | None = remaining(end)
            if budget is not None and budget <= 0:
                return False
            self.token.wait(
                self._poll_interval if budget is None else min(self._poll_interval, budget)
            )

    async def wait_async(
        self: ShutdownManager,
        timeout: float | None = None,
    ) -> str:
        """
        Await a shutdown request while periodically draining operating-system signals.

        Args:
            timeout: Maximum wait in seconds, or None for no deadline.
        """
        self._guard.check()
        deadline(timeout)
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        timer: asyncio.TimerHandle | None = None

        def poll_signals() -> None:
            """
            Drain deferred signals and schedule the next poll when needed.
            """
            nonlocal timer
            self.poll()
            if not self.token.cancelled:
                timer = loop.call_later(self._poll_interval, poll_signals)

        poll_signals()
        try:
            return await self.token.wait_async(timeout=timeout)
        finally:
            if timer is not None:
                timer.cancel()

    def register(
        self: ShutdownManager,
        name: str,
        callback: Callable[[ShutdownContext], Any],
        *,
        phase: int = Phase.CLOSE,
        priority: int = 0,
        timeout: float | None = None,
        after: Iterable[str] = (),
    ) -> None:
        """
        Register a named cleanup hook and its execution constraints.

        Args:
            name: Name identifying this instance or registered resource.
            callback: Callable invoked for matching events or during cleanup.
            phase: Cleanup phase; lower phase values run first.
            priority: Ordering priority; larger values run earlier within the same phase.
            timeout: Maximum wait in seconds, or None for no deadline.
            after: Names of hooks that must be attempted before this hook.
        """
        if not callable(callback):
            raise TypeError("callback must be callable")
        deadline(timeout)
        self._guard.check()
        dependencies: tuple[str, ...] = tuple(after)
        with self._lock:
            if self._state in {State.RUNNING, State.CLOSED}:
                raise RuntimeError("Cannot register hooks after cleanup starts")
            if name in self._hooks:
                raise ValueError(f"Duplicate shutdown hook: {name}")
            self._sequence += 1
            self._hooks[name] = Hook(
                name, callback, phase, priority, self._sequence, timeout, dependencies
            )

    def unregister(
        self: ShutdownManager,
        name: str,
    ) -> bool:
        """
        Remove a cleanup hook before execution begins.

        Args:
            name: Name identifying this instance or registered resource.
        """
        self._guard.check()
        with self._lock:
            if self._state in {State.RUNNING, State.CLOSED}:
                raise RuntimeError("Cannot unregister hooks after cleanup starts")
            return self._hooks.pop(name, None) is not None

    def _begin(
        self: ShutdownManager,
        reason: str,
    ) -> tuple[Hook, ...] | ShutdownReport:
        """
        Validate the hook plan and claim cleanup execution once.

        Args:
            reason: First-cause description retained when cancellation is requested.
        """
        self.poll()
        self._guard.check()
        with self._lock:
            if self._report is not None:
                return self._report
            if self._state is State.RUNNING:
                raise RuntimeError("Shutdown is already running")
            hooks: tuple[Hook, ...] = plan(self._hooks.values())
            if not self.token.cancelled:
                self.request(reason)
            self._state = State.RUNNING
            self._runner_thread = threading.get_ident()
            try:
                self._runner_task = asyncio.current_task()
            except RuntimeError:
                self._runner_task = None
            return hooks

    def _complete(
        self: ShutdownManager,
        runner: HookRunner,
    ) -> ShutdownReport:
        """
        Cache the cleanup report and notify completion observers.

        Args:
            runner: Cleanup execution whose outcomes are being finalized.
        """
        self.poll()
        with self._lock:
            self._report = ShutdownReport(
                self._reason(),
                runner.finish(),
                time.perf_counter() - runner.started,
                self._escalated,
                runner.interrupted,
            )
            self._state = State.CLOSED
            self._runner_thread = None
            self._runner_task = None
            self._hooks.clear()
            self._completion.cancel(self._reason())
            return self._report

    @property
    def planned_hooks(
        self: ShutdownManager,
    ) -> tuple[str, ...]:
        """
        Validate dependencies and return the planned hook names.
        """
        self._guard.check()
        with self._lock:
            return tuple(hook.name for hook in plan(self._hooks.values()))

    def wait_closed(
        self: ShutdownManager,
        timeout: float | None = None,
    ) -> ShutdownReport:
        """
        Wait from another thread for the cached report; does not start cleanup.

        Args:
            timeout: Maximum wait in seconds, or None for no deadline.
        """
        self._guard.check()
        with self._lock:
            if self._runner_thread == threading.get_ident():
                raise RuntimeError("Cannot synchronously wait for cleanup on its executing thread")
        if not self._completion.wait(timeout):
            raise TimeoutError("Shutdown has not completed")
        with self._lock:
            assert self._report is not None
            return self._report

    async def wait_closed_async(
        self: ShutdownManager,
        timeout: float | None = None,
    ) -> ShutdownReport:
        """
        Await the cached cleanup report without starting cleanup.

        Args:
            timeout: Maximum wait in seconds, or None for no deadline.
        """
        self._guard.check()
        with self._lock:
            if self._runner_task is asyncio.current_task():
                raise RuntimeError("Cleanup cannot wait for its own completion")
        await self._completion.wait_async(timeout=timeout)
        with self._lock:
            assert self._report is not None
            return self._report

    def shutdown(
        self: ShutdownManager,
        reason: str = "shutdown",
    ) -> ShutdownReport:
        """
        Run synchronous cleanup hooks once and return their report.

        Args:
            reason: First-cause description retained when cancellation is requested.
        """
        hooks: tuple[Hook, ...] | ShutdownReport = self._begin(reason)
        if isinstance(hooks, ShutdownReport):
            return hooks
        runner: HookRunner = HookRunner(hooks, self.token, self._timeout, self.poll)
        try:
            runner.run()
        finally:
            report: ShutdownReport = self._complete(runner)
        return report

    async def shutdown_async(
        self: ShutdownManager,
        reason: str = "shutdown",
    ) -> ShutdownReport:
        """
        Await cleanup hooks once and return their report.

        Args:
            reason: First-cause description retained when cancellation is requested.
        """
        hooks: tuple[Hook, ...] | ShutdownReport = self._begin(reason)
        if isinstance(hooks, ShutdownReport):
            return hooks
        runner: HookRunner = HookRunner(hooks, self.token, self._timeout, self.poll)
        try:
            await runner.run_async()
        finally:
            report: ShutdownReport = self._complete(runner)
        return report
