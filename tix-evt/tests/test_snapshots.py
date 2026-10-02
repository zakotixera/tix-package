"""
Bounded snapshot reuse under subscription mutation and weak callback expiry.
"""

from __future__ import annotations

import gc

from evt import ANY, Event, EventBus, Subscription


def test_wildcard_cache_preserves_order_and_invalidates_on_changes() -> None:
    """
    Cached dispatch reflects additions, removals, one-shot claims, and clear.
    """
    bus: EventBus = EventBus()
    calls: list[str] = []
    wildcard: Subscription = bus.subscribe(ANY, lambda event: calls.append("any"))
    bus.subscribe("work", lambda event: calls.append("exact"))
    bus.emit("work")
    bus.emit("work")
    assert calls == ["any", "exact"] * 2
    calls.clear()
    bus.subscribe("work", lambda event: calls.append("once"), priority=1, once=True)
    bus.emit("work")
    wildcard.cancel()
    bus.emit("work")
    assert calls == ["once", "any", "exact", "exact"]
    bus.clear()
    calls.clear()
    bus.emit("work")
    assert calls == []


def test_arbitrary_topics_do_not_grow_the_snapshot_cache() -> None:
    """
    Only topics with exact subscriptions retain merged snapshots.
    """
    bus: EventBus = EventBus()
    bus.subscribe(ANY, lambda event: None)
    bus.subscribe("work", lambda event: None)
    bus.emit("work")
    topic: int
    for topic in range(1000):
        bus.emit(topic)
    assert len(bus._registry._snapshots) == 1
    bus.clear()
    assert not bus._registry._snapshots


def test_cached_snapshot_does_not_keep_weak_callbacks_alive() -> None:
    """
    Weak-reference expiry removes cached entries before subsequent dispatch.
    """
    calls: list[str] = []

    class Receiver:
        """
        Receive events through a weakly referenced bound method.
        """

        def receive(
            self: Receiver,
            event: Event,
        ) -> None:
            """
            Record a callback invocation.

            Args:
                event: Event delivered to the receiver.
            """
            calls.append("weak")

    bus: EventBus = EventBus()
    receiver: Receiver = Receiver()
    bus.subscribe(ANY, lambda event: calls.append("any"))
    bus.subscribe("work", receiver.receive, weak=True)
    bus.emit("work")
    del receiver
    gc.collect()
    bus.emit("work")
    assert calls == ["any", "weak", "any"]
    assert bus.listener_count == 1
