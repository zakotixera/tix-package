"""
Cleanup planning and execution, independent of lifecycle and signal ownership.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from ._runtime import deadline, remaining
from .models import HookResult, ShutdownContext
from .token import CancellationToken


@dataclass(frozen=True, slots=True)
class Hook:
    """
    Immutable cleanup registration including ordering and deadline constraints.
    """

    name: str
    callback: Callable[[ShutdownContext], Any]
    phase: int
    priority: int
    sequence: int
    timeout: float | None
    after: tuple[str, ...]


def plan(
    hooks: Iterable[Hook],
) -> tuple[Hook, ...]:
    """
    Topologically order within phases with stable priority/LIFO tie breaking.

    Args:
        hooks: Cleanup hooks to validate, order, or execute.
    """
    pending: dict[str, Hook] = {hook.name: hook for hook in hooks}
    hook: Hook
    for hook in pending.values():
        dependency: str
        for dependency in hook.after:
            if dependency not in pending:
                raise ValueError(f"Unknown cleanup dependency {dependency!r} for {hook.name!r}")
            if pending[dependency].phase > hook.phase:
                raise ValueError("Cleanup dependencies cannot point to a later phase")
    ordered: list[Hook] = []
    done: set[str] = set()
    while pending:
        phase: int = min(hook.phase for hook in pending.values())
        ready: list[Hook] = [
            hook
            for hook in pending.values()
            if hook.phase == phase and all(name in done for name in hook.after)
        ]
        if not ready:
            raise ValueError("Cycle in cleanup dependencies")
        hook = min(ready, key=lambda item: (-item.priority, -item.sequence))
        ordered.append(hook)
        done.add(hook.name)
        del pending[hook.name]
    return tuple(ordered)


class HookRunner:
    """
    Execute a validated cleanup plan and collect per-hook outcomes.
    """

    def __init__(
        self: HookRunner,
        hooks: tuple[Hook, ...],
        token: CancellationToken,
        timeout: float | None,
        poll: Callable[[], int],
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            hooks: Cleanup hooks to validate, order, or execute.
            token: Token.
            timeout: Maximum wait in seconds, or None for no deadline.
            poll: Callback draining pending operating-system signal requests.
        """
        self.hooks: tuple[Hook, ...] = hooks
        self.token: CancellationToken = token
        self.poll: Callable[[], int] = poll
        self.started: float = time.perf_counter()
        self.end: float | None = deadline(timeout)
        self.results: list[HookResult] = []
        self.interrupted: bool = True

    def _context(
        self: HookRunner,
        hook: Hook,
    ) -> ShutdownContext:
        """
        Build the context with the tighter hook or overall deadline.

        Args:
            hook: Cleanup hook being prepared or recorded.
        """
        end: float | None = deadline(hook.timeout)
        if self.end is not None:
            end = self.end if end is None else min(end, self.end)
        reason: str | None = self.token.reason
        return ShutdownContext(self.token, reason if reason is not None else "shutdown", end)

    def _prepare(
        self: HookRunner,
        hook: Hook,
    ) -> ShutdownContext | None:
        """
        Poll requests and skip hooks whose budget is already exhausted.

        Args:
            hook: Cleanup hook being prepared or recorded.
        """
        self.poll()
        context: ShutdownContext = self._context(hook)
        if context.remaining == 0:
            self.results.append(HookResult(hook.name, 0, TimeoutError("Budget exhausted"), True))
            return None
        return context

    def _record(
        self: HookRunner,
        hook: Hook,
        start: float,
        error: Exception | None,
    ) -> None:
        """
        Append the elapsed time and outcome of one hook.

        Args:
            hook: Cleanup hook being prepared or recorded.
            start: Perf-counter timestamp captured before hook execution.
            error: Failure associated with this operation.
        """
        self.results.append(HookResult(hook.name, time.perf_counter() - start, error))

    def run(
        self: HookRunner,
    ) -> None:
        """
        Execute the assigned synchronous work.
        """
        hook: Hook
        for hook in self.hooks:
            context: ShutdownContext | None = self._prepare(hook)
            if context is None:
                continue
            start: float = time.perf_counter()
            error: Exception | None = None
            caught: Exception
            try:
                value: Any = hook.callback(context)
                if inspect.isawaitable(value):
                    if inspect.iscoroutine(value):
                        value.close()
                    raise TypeError("Async hook requires shutdown_async()")
                if context.remaining == 0:
                    raise TimeoutError("Synchronous hook exceeded its cooperative budget")
            except Exception as caught:
                error = caught
            except BaseException:
                self._record(hook, start, RuntimeError("Hook interrupted"))
                raise
            self._record(hook, start, error)
        self.interrupted = False

    async def run_async(
        self: HookRunner,
    ) -> None:
        """
        Execute cleanup hooks with cooperative asynchronous deadlines.
        """
        hook: Hook
        for hook in self.hooks:
            context: ShutdownContext | None = self._prepare(hook)
            if context is None:
                continue
            start: float = time.perf_counter()
            error: Exception | None = None
            caught: Exception
            try:
                async with asyncio.timeout(remaining(context.deadline)):
                    value: Any = hook.callback(context)
                    if inspect.isawaitable(value):
                        await value
                if context.remaining == 0:
                    raise TimeoutError("Hook exceeded its cooperative budget")
            except Exception as caught:
                error = caught
            except BaseException:
                self._record(hook, start, RuntimeError("Hook interrupted"))
                raise
            self._record(hook, start, error)
        self.interrupted = False

    def finish(
        self: HookRunner,
    ) -> tuple[HookResult, ...]:
        """
        Return all outcomes, marking hooks left unattempted by interruption.
        """
        if self.interrupted:
            completed: set[str] = {result.name for result in self.results}
            self.results.extend(
                HookResult(hook.name, 0, RuntimeError("Shutdown interrupted"), True)
                for hook in self.hooks
                if hook.name not in completed
            )
        return tuple(self.results)
