"""
Composable event transformations with bounded, synchronized state.
"""

from __future__ import annotations

import os
import threading
import time
from _thread import LockType
from collections import OrderedDict
from collections.abc import Callable, Hashable
from typing import Any

from ._runtime import ProcessGuard, deadline


def execution_context(
    event: dict[str, Any],
) -> dict[str, Any]:
    """
    Attach producer process/thread identity before handing a record to another worker.

    Args:
        event: Event being transformed, selected, or delivered.
    """
    return {
        **event,
        "process_id": os.getpid(),
        "thread_id": threading.get_ident(),
        "thread_name": threading.current_thread().name,
    }


class RateLimit:
    """
    Per-key token buckets with bounded LRU storage and a total dropped count.

    A fresh key starts with ``burst`` tokens and refills at ``rate`` events/second.
    Eviction resets that key's budget. This is load shedding, not an audit/security
    rate limiter. The key function runs outside the state lock.
    """

    def __init__(
        self: RateLimit,
        rate: float,
        *,
        burst: int = 1,
        capacity: int = 1024,
        key: Callable[[dict[str, Any]], Hashable] | None = None,
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            rate: Number of tokens replenished per second.
            burst: Maximum tokens available for a single key.
            capacity: Maximum number of buffered records or retained keys.
            key: Optional function deriving a bucket key from the event.
        """
        deadline(rate)
        if (
            rate <= 0
            or not isinstance(burst, int)
            or burst < 1
            or not isinstance(capacity, int)
            or capacity < 1
        ):
            raise ValueError("rate, burst and capacity must be positive")
        self._guard: ProcessGuard = ProcessGuard()
        self._rate: float = rate
        self._burst: int = burst
        self._capacity: int = capacity
        self._key: Callable[[dict[str, Any]], Hashable] = key or (
            lambda event: (event["logger"], event["message"])
        )
        self._buckets: OrderedDict[Hashable, tuple[float, float]] = OrderedDict()
        self._lock: LockType = threading.Lock()
        self._dropped: int = 0

    @property
    def dropped(
        self: RateLimit,
    ) -> int:
        """
        Return the synchronized count of events rejected by rate limiting.
        """
        self._guard.check()
        with self._lock:
            return self._dropped

    def __call__(
        self: RateLimit,
        event: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Apply the processor and return the accepted event or None.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        self._guard.check()
        key: Hashable = self._key(event)
        with self._lock:
            now: float = time.monotonic()
            tokens: float
            previous: float
            tokens, previous = self._buckets.pop(key, (float(self._burst), now))
            tokens = min(self._burst, tokens + (now - previous) * self._rate)
            allowed: bool = tokens >= 1
            self._buckets[key] = (tokens - 1 if allowed else tokens, now)
            if len(self._buckets) > self._capacity:
                self._buckets.popitem(last=False)
            if not allowed:
                self._dropped += 1
                return None
        return event
