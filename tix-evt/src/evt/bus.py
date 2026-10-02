"""
Snapshot registry with explicit synchronous and asynchronous dispatch.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Coroutine, Hashable, Iterator, Mapping
from concurrent.futures import Future
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any, Literal, TypeAlias

from ._registry import Registry, _Entry
from ._runtime import ProcessGuard, deadline
from .models import ANY, DispatchResult, Event, Outcome, Subscription

Handler: TypeAlias = Callable[[Event], Any]
ErrorPolicy: TypeAlias = Literal["collect", "raise"]


class EventBus:
    """
    Thread-safe registry. Callbacks themselves must support their execution context.
    """

    def __init__(
        self: EventBus,
        *,
        errors: ErrorPolicy = "raise",
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            errors: Policy for collecting or raising ordinary callback failures.
        """
        if errors not in {"collect", "raise"}:
            raise ValueError("Unknown error policy")
        self._errors: ErrorPolicy = errors
        self._guard: ProcessGuard = ProcessGuard()
        self._registry: Registry = Registry()
        self._muted: ContextVar[bool] = ContextVar("evt.muted", default=False)

    def subscribe(
        self: EventBus,
        topic: Hashable,
        callback: Handler,
        *,
        priority: int = 0,
        once: bool = False,
        weak: bool = False,
        sender: object = ANY,
        predicate: Callable[[Event], bool] | None = None,
    ) -> Subscription:
        """
        Register a callback and return its disconnect handle.

        Args:
            topic: Hashable topic used to route the event or subscription.
            callback: Callable invoked for matching events or during cleanup.
            priority: Delivery priority; larger values run before smaller values.
            once: Whether to remove the subscription before its first invocation.
            weak: Whether to retain the callback through a weak reference.
            sender: Sender identity to attach or match; ANY matches all senders.
            predicate: Optional synchronous predicate selecting matching events.
        """
        return self._registry.add(
            topic,
            callback,
            priority=priority,
            once=once,
            weak=weak,
            sender=sender,
            predicate=predicate,
        )

    @property
    def listener_count(
        self: EventBus,
    ) -> int:
        """
        Return the number of active subscriptions.
        """
        return self._registry.listener_count

    def clear(
        self: EventBus,
    ) -> None:
        """
        Remove all registered subscriptions.
        """
        self._registry.clear()

    def listeners(
        self: EventBus,
        topic: Hashable,
    ) -> tuple[int, ...]:
        """
        Snapshot subscription IDs matching an exact topic, including ANY.

        Args:
            topic: Hashable topic used to route the event or subscription.
        """
        return tuple(entry.id for entry in self._registry.snapshot(topic))

    def submit(
        self: EventBus,
        loop: asyncio.AbstractEventLoop,
        topic: Hashable,
        payload: Any = None,
        *,
        sender: object = None,
        metadata: Mapping[str, Any] | None = None,
        concurrency: int = 1,
    ) -> Future[DispatchResult]:
        """
        Submit from an ordinary thread; cancellation forwards to dispatch.

        The caller owns loop lifetime. Do not block on the returned future in that
        loop's thread. Payload contents remain caller-owned until dispatch finishes.

        Args:
            loop: Running event loop that owns submitted work or its future.
            topic: Hashable topic used to route the event or subscription.
            payload: Caller-owned event data passed to subscribers.
            sender: Sender identity to attach or match; ANY matches all senders.
            metadata: Additional event metadata, copied into the envelope.
            concurrency: Maximum number of callback workers for this emission.
        """
        self._guard.check()
        if not loop.is_running() or loop.is_closed():
            raise RuntimeError("Target event loop must be running")
        coroutine: Coroutine[Any, Any, DispatchResult] = self.emit_async(
            topic, payload, sender=sender, metadata=dict(metadata or {}), concurrency=concurrency
        )
        try:
            return asyncio.run_coroutine_threadsafe(coroutine, loop)
        except BaseException:
            coroutine.close()
            raise

    async def wait_for(
        self: EventBus,
        topic: Hashable,
        *,
        predicate: Callable[[Event], bool] | None = None,
        sender: object = ANY,
        timeout: float | None = None,
    ) -> Event:
        """
        Wait for the next matching event; disconnect on timeout or cancellation.

        Registration occurs when the coroutine starts. Delivery may originate on
        another thread; predicates execute on the publishing thread.

        Args:
            topic: Hashable topic used to route the event or subscription.
            predicate: Optional synchronous predicate selecting matching events.
            sender: Sender identity to attach or match; ANY matches all senders.
            timeout: Maximum wait in seconds, or None for no deadline.
        """
        self._guard.check()
        deadline(timeout)
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        future: asyncio.Future[Event] = loop.create_future()

        def deliver(
            event: Event,
        ) -> None:
            """
            Resolve the pending event waiter on its owning loop.

            Args:
                event: Event being transformed, selected, or delivered.
            """
            if not future.done():
                future.set_result(event)

        def receive(
            event: Event,
        ) -> None:
            """
            Forward a matching event to the waiting consumer.

            Args:
                event: Event being transformed, selected, or delivered.
            """
            loop.call_soon_threadsafe(deliver, event)

        with self.subscribe(topic, receive, once=True, sender=sender, predicate=predicate):
            async with asyncio.timeout(timeout):
                return await future

    @contextmanager
    def muted(
        self: EventBus,
    ) -> Iterator[None]:
        """
        Suppress emissions in this context until the managed scope exits.
        """
        token: Token[bool] = self._muted.set(True)
        try:
            yield
        finally:
            self._muted.reset(token)

    def _snapshot(
        self: EventBus,
        topic: Hashable,
    ) -> tuple[_Entry, ...]:
        """
        Select the subscription snapshot visible to this emission.

        Args:
            topic: Hashable topic used to route the event or subscription.
        """
        self._guard.check()
        if self._muted.get():
            return ()
        return self._registry.snapshot(topic)

    def _finish(
        self: EventBus,
        outcomes: list[Outcome],
    ) -> DispatchResult:
        """
        Build the dispatch result and apply its error policy.

        Args:
            outcomes: Ordered callback results collected during dispatch.
        """
        result: DispatchResult = DispatchResult(tuple(outcomes))
        if self._errors == "raise":
            result.raise_errors()
        return result

    def emit(
        self: EventBus,
        topic: Hashable,
        payload: Any = None,
        *,
        sender: object = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> DispatchResult:
        """
        Invoke matching synchronous subscribers and collect their outcomes.

        Args:
            topic: Hashable topic used to route the event or subscription.
            payload: Caller-owned event data passed to subscribers.
            sender: Sender identity to attach or match; ANY matches all senders.
            metadata: Additional event metadata, copied into the envelope.
        """
        entries: tuple[_Entry, ...] = self._snapshot(topic)
        if not entries:
            return DispatchResult()
        event: Event = Event(topic, payload, sender, metadata or {})
        outcomes: list[Outcome] = []
        entry: _Entry
        for entry in entries:
            error: Exception
            try:
                callback: Callable[[Event], Any] | None = self._registry.select(entry, event)
                if callback is None:
                    continue
                value: Any = callback(event)
                if value is not None and inspect.isawaitable(value):
                    if inspect.iscoroutine(value):
                        value.close()
                    raise TypeError("Async handler requires emit_async()")
                outcomes.append(Outcome(entry.id, value))
            except Exception as error:
                outcomes.append(Outcome(entry.id, error=error))
        return self._finish(outcomes)

    async def emit_async(
        self: EventBus,
        topic: Hashable,
        payload: Any = None,
        *,
        sender: object = None,
        metadata: Mapping[str, Any] | None = None,
        concurrency: int = 1,
    ) -> DispatchResult:
        """
        Await matching callbacks with bounded concurrency and ordered results.

        Args:
            topic: Hashable topic used to route the event or subscription.
            payload: Caller-owned event data passed to subscribers.
            sender: Sender identity to attach or match; ANY matches all senders.
            metadata: Additional event metadata, copied into the envelope.
            concurrency: Maximum number of callback workers for this emission.
        """
        if concurrency < 1:
            raise ValueError("concurrency must be positive")
        entries: tuple[_Entry, ...] = self._snapshot(topic)
        if not entries:
            return DispatchResult()
        event: Event = Event(topic, payload, sender, metadata or {})
        outcomes: list[Outcome | None] = [None] * len(entries)
        pending: Iterator[tuple[int, _Entry]] = iter(enumerate(entries))

        async def worker() -> None:
            """
            Process queued work until the assigned input is exhausted.
            """
            index: int
            entry: _Entry
            for index, entry in pending:
                error: Exception
                try:
                    callback: Callable[[Event], Any] | None = self._registry.select(entry, event)
                    if callback is None:
                        continue
                    value: Any = callback(event)
                    if value is not None and inspect.isawaitable(value):
                        value = await value
                    outcomes[index] = Outcome(entry.id, value)
                except Exception as error:
                    outcomes[index] = Outcome(entry.id, error=error)

        if concurrency == 1:
            await worker()
        else:
            tasks: list[asyncio.Task[None]] = [
                asyncio.create_task(worker()) for _ in range(min(concurrency, len(entries)))
            ]
            try:
                await asyncio.gather(*tasks)
            finally:
                task: asyncio.Task[None]
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        return self._finish([outcome for outcome in outcomes if outcome is not None])
