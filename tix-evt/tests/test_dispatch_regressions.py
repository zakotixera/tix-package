"""
Behavioral regressions for subscription insertion, cache invalidation, and dispatch.
"""

from __future__ import annotations

import asyncio
import gc
import weakref
from collections.abc import Callable, Generator
from typing import Any

import pytest
from evt import ANY, DispatchResult, Event, EventBus, Outcome, Subscription


class FalsyAwaitable:
    """
    Return a value after suspension despite evaluating as false.
    """

    def __bool__(
        self: FalsyAwaitable,
    ) -> bool:
        """
        Evaluate as false without changing awaitability.
        """
        return False

    def __await__(
        self: FalsyAwaitable,
    ) -> Generator[Any, None, int]:
        """
        Suspend once and return the callback result.
        """
        yield from asyncio.sleep(0).__await__()
        return 42


def test_priority_insertion_and_replacement_preserve_ties() -> None:
    """
    Interleaved priorities retain registration order across wildcard merges.
    """
    bus: EventBus = EventBus()
    handles: list[Subscription] = []
    index: int
    priority: int
    for index, priority in enumerate((-3, 7, 0, 7, -3, 0)):
        handles.append(
            bus.subscribe("work" if index % 2 else ANY, lambda event: None, priority=priority)
        )
    assert bus.listeners("work") == (2, 4, 3, 6, 1, 5)
    handles[3].cancel()
    bus.subscribe("work", lambda event: None, priority=7)
    result: DispatchResult = bus.emit("work")
    assert result.outcomes == (
        Outcome(2),
        Outcome(7),
        Outcome(3),
        Outcome(6),
        Outcome(1),
        Outcome(5),
    )


def test_topic_and_wildcard_changes_refresh_all_affected_results() -> None:
    """
    Independently cached topics reflect exact and wildcard mutations.
    """
    bus: EventBus = EventBus()
    wildcard: Subscription = bus.subscribe(ANY, lambda event: "shared")
    bus.subscribe("a", lambda event: "a")
    bus.subscribe("b", lambda event: "b")
    assert bus.listeners("a") == (1, 2)
    assert bus.listeners("b") == (1, 3)
    once: Subscription = bus.subscribe("a", lambda event: "once", once=True, priority=1)
    assert bus.listeners("a") == (once.id, 1, 2)
    assert bus.listeners("b") == (1, 3)
    assert bus.emit("a").outcomes == (
        Outcome(once.id, "once"),
        Outcome(wildcard.id, "shared"),
        Outcome(2, "a"),
    )
    assert bus.listeners("a") == (1, 2)
    wildcard.cancel()
    assert bus.listeners("a") == (2,)
    assert bus.listeners("b") == (3,)
    replacement: Subscription = bus.subscribe(ANY, lambda event: "new", priority=2)
    assert bus.listeners("a") == (replacement.id, 2)
    assert bus.listeners("b") == (replacement.id, 3)


def test_cancel_releases_strong_callback_from_cached_snapshot() -> None:
    """
    Removing an exact subscription releases its callback while other topics remain.
    """
    bus: EventBus = EventBus()

    def receive(
        event: Event,
    ) -> None:
        """
        Receive an event through a strongly retained callback.

        Args:
            event: Envelope delivered by the bus.
        """

    reference: weakref.ReferenceType[Callable[[Event], None]] = weakref.ref(receive)
    handle: Subscription = bus.subscribe("a", receive)
    bus.subscribe("b", lambda event: None)
    bus.subscribe(ANY, lambda event: None)
    bus.listeners("a")
    bus.listeners("b")
    del receive
    assert reference() is not None
    handle.cancel()
    gc.collect()
    assert reference() is None
    assert bus.listeners("b") == (2, 3)


def test_sync_dispatch_rejects_falsy_awaitable_and_keeps_none_result() -> None:
    """
    Awaitability checks reject false-valued awaitables and preserve None outcomes.
    """
    bus: EventBus = EventBus(errors="collect")
    bus.subscribe("work", lambda event: FalsyAwaitable())
    bus.subscribe("work", lambda event: None)
    result: DispatchResult = bus.emit("work")
    assert len(result.outcomes) == 2
    assert isinstance(result.outcomes[0].error, TypeError)
    assert result.outcomes[1].value is None
    assert result.outcomes[1].error is None


@pytest.mark.parametrize("concurrency", [1, 3])
async def test_async_dispatch_awaits_falsy_values_and_keeps_none_result(
    concurrency: int,
) -> None:
    """
    Await false-valued results in both serial and concurrent dispatch.

    Args:
        concurrency: Maximum worker count for this emission.
    """
    bus: EventBus = EventBus()
    bus.subscribe("work", lambda event: FalsyAwaitable())
    bus.subscribe("work", lambda event: None)
    result: DispatchResult = await bus.emit_async("work", concurrency=concurrency)
    assert result.outcomes == (Outcome(1, 42), Outcome(2))
    assert result.errors == ()
