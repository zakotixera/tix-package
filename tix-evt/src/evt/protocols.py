"""
Structural contracts for application adapters; no inheritance required.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable
from typing import Any, Protocol

from .models import ANY, Event, Subscription


class EventSource(
    Protocol,
):
    """
    Structural subscription contract accepted by event stream adapters.
    """

    def subscribe(
        self: EventSource,
        topic: Hashable,
        callback: Callable[[Event], Any],
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
        ...
