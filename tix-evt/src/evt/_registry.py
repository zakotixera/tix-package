"""
Subscription ownership and snapshot indexing, independent of execution.
"""

from __future__ import annotations

import inspect
import threading
import weakref
from _thread import RLock
from bisect import bisect_right
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from typing import Any, TypeAlias, cast

from ._runtime import ProcessGuard
from .models import ANY, Event, Subscription

Handler: TypeAlias = Callable[[Event], Any]


@dataclass(frozen=True, slots=True)
class _Entry:
    """
    Immutable subscription configuration used in dispatch snapshots.
    """

    id: int
    topic: Hashable
    callback: Handler | weakref.ReferenceType[Handler]
    weak: bool
    priority: int
    once: bool
    sender: object
    predicate: Callable[[Event], bool] | None

    def resolve(
        self: _Entry,
    ) -> Handler | None:
        """
        Return the live callback, or None after weak-reference expiry.
        """
        if self.weak:
            assert isinstance(self.callback, weakref.ReferenceType)
            return self.callback()
        return cast(Handler, self.callback)


class Registry:
    """
    Own subscription indexing, weak references, and atomic one-shot claims.
    """

    def __init__(
        self: Registry,
    ) -> None:
        """
        Initialize the instance and its owned state.
        """
        self._guard: ProcessGuard = ProcessGuard()
        self._lock: RLock = threading.RLock()
        self._topics: dict[Hashable, tuple[_Entry, ...]] = {}
        self._entries: dict[int, _Entry] = {}
        self._snapshots: dict[Hashable, tuple[_Entry, ...]] = {}
        self._sequence: int = 0

    def add(
        self: Registry,
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
        Insert a subscription into the priority-ordered registry.

        Args:
            topic: Hashable topic used to route the event or subscription.
            callback: Callable invoked for matching events or during cleanup.
            priority: Delivery priority; larger values run before smaller values.
            once: Whether to remove the subscription before its first invocation.
            weak: Whether to retain the callback through a weak reference.
            sender: Sender identity to attach or match; ANY matches all senders.
            predicate: Optional synchronous predicate selecting matching events.
        """
        self._guard.check()
        hash(topic)
        if not callable(callback):
            raise TypeError("callback must be callable")
        with self._lock:
            self._sequence += 1
            identifier: int = self._sequence
            target: Handler | weakref.ReferenceType[Handler] = callback
            if weak:
                bus_ref: weakref.ReferenceType[Registry] = weakref.ref(self)

                def gone(
                    _: object,
                ) -> None:
                    """
                    Remove a subscription after its weakly referenced callback expires.

                    Args:
                        _: Unused notification or iteration value.
                    """
                    bus: Registry | None = bus_ref()
                    if bus is not None and bus._guard.owned:
                        bus.remove(identifier)

                target = (
                    weakref.WeakMethod(callback, gone)
                    if inspect.ismethod(callback)
                    else weakref.ref(callback, gone)
                )
            entry: _Entry = _Entry(
                identifier, topic, target, weak, priority, once, sender, predicate
            )
            self._entries[identifier] = entry
            entries: list[_Entry] = list(self._topics.get(topic, ()))
            # New IDs follow existing IDs at equal priority.
            index: int = bisect_right(entries, -priority, key=lambda item: -item.priority)
            entries.insert(index, entry)
            self._topics[topic] = tuple(entries)
            if topic is ANY:
                self._snapshots.clear()
            else:
                self._snapshots.pop(topic, None)
            return Subscription(identifier, self.remove)

    def remove(
        self: Registry,
        identifier: int,
    ) -> bool:
        """
        Disconnect the subscription identified by its handle.

        Args:
            identifier: Identifier of the subscription or worker.
        """
        self._guard.check()
        with self._lock:
            entry: _Entry | None = self._entries.pop(identifier, None)
            if entry is None:
                return False
            if entry.topic is ANY:
                self._snapshots.clear()
            else:
                self._snapshots.pop(entry.topic, None)
            remaining: tuple[_Entry, ...] = tuple(
                item for item in self._topics[entry.topic] if item.id != identifier
            )
            if remaining:
                self._topics[entry.topic] = remaining
            else:
                del self._topics[entry.topic]
            return True

    @property
    def listener_count(
        self: Registry,
    ) -> int:
        """
        Return the number of active subscriptions.
        """
        self._guard.check()
        with self._lock:
            return len(self._entries)

    def clear(
        self: Registry,
    ) -> None:
        """
        Remove all registered subscriptions.
        """
        self._guard.check()
        with self._lock:
            entries: dict[int, _Entry]
            topics: dict[Hashable, tuple[_Entry, ...]]
            entries, topics = self._entries, self._topics
            self._entries, self._topics = {}, {}
            snapshots: dict[Hashable, tuple[_Entry, ...]] = self._snapshots
            self._snapshots = {}
        # Release callback references outside the lock: destructors may reenter.
        entries.clear()
        topics.clear()
        snapshots.clear()

    def snapshot(
        self: Registry,
        topic: Hashable,
    ) -> tuple[_Entry, ...]:
        """
        Return the ordered exact-topic and wildcard subscriptions.

        Args:
            topic: Hashable topic used to route the event or subscription.
        """
        if topic is ANY:
            raise ValueError("ANY is a subscription selector, not an event topic")
        self._guard.check()
        with self._lock:
            exact: tuple[_Entry, ...] = self._topics.get(topic, ())
            wildcard: tuple[_Entry, ...] = self._topics.get(ANY, ())
            if not wildcard:
                return exact
            if not exact:
                return wildcard
            # Only subscribed topics are cached; arbitrary emitted topics cannot grow it.
            snapshot: tuple[_Entry, ...] | None = self._snapshots.get(topic)
            if snapshot is None:
                snapshot = tuple(
                    sorted(exact + wildcard, key=lambda item: (-item.priority, item.id))
                )
                self._snapshots[topic] = snapshot
            return snapshot

    def select(
        self: Registry,
        entry: _Entry,
        event: Event,
    ) -> Handler | None:
        """
        Apply sender and predicate filters, then claim one-shot delivery.

        Args:
            entry: Registered subscription being considered for delivery.
            event: Event being transformed, selected, or delivered.
        """
        callback: Callable[[Event], Any] | None = entry.resolve()
        if callback is None or (entry.sender is not ANY and entry.sender is not event.sender):
            return None
        if entry.predicate is not None and not entry.predicate(event):
            return None
        if entry.once and not self.remove(entry.id):
            return None
        return callback
