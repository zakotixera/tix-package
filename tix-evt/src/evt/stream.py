"""
A bounded, event-loop-local async iteration adapter.
"""

from __future__ import annotations

import asyncio
from collections.abc import Hashable
from types import TracebackType
from typing import Literal

from evt import Subscription

from ._runtime import ProcessGuard
from .models import Event
from .protocols import EventSource


class EventStream:
    """
    Subscribe immediately in a running loop; close explicitly or use async with.

    A stream has one consumer. ``aclose`` discards queued events and wakes a pending
    consumer. Publish on the owning loop; marshal other threads with call_soon_threadsafe.
    """

    def __init__(
        self: EventStream,
        bus: EventSource,
        topic: Hashable,
        *,
        capacity: int = 128,
        overflow: Literal["drop_oldest", "drop_newest", "raise"] = "drop_oldest",
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            bus: Event source supplying the stream subscription.
            topic: Hashable topic used to route the event or subscription.
            capacity: Maximum number of buffered records or retained keys.
            overflow: Policy applied when the bounded buffer is full.
        """
        if capacity < 1:
            raise ValueError("capacity must be positive")
        if overflow not in {"drop_oldest", "drop_newest", "raise"}:
            raise ValueError("Unknown overflow policy")
        self._loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        self._guard: ProcessGuard = ProcessGuard()
        self._queue: asyncio.Queue[Event | None] = asyncio.Queue(capacity)
        self._overflow: Literal["drop_oldest", "drop_newest", "raise"] = overflow
        self._closed: bool = False
        self._reading: bool = False
        self.dropped: int = 0
        self._subscription: Subscription = bus.subscribe(topic, self._receive, weak=True)

    def _check_loop(
        self: EventStream,
    ) -> None:
        """
        Reject access outside the stream owning process or event loop.
        """
        self._guard.check()
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("EventStream must be used on its owning event loop")

    def _receive(
        self: EventStream,
        event: Event,
    ) -> None:
        """
        Accept a notification for deferred or buffered delivery.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        self._check_loop()
        if self._closed:
            return
        if self._queue.full():
            self.dropped += 1
            if self._overflow == "raise":
                raise asyncio.QueueFull
            if self._overflow == "drop_newest":
                return
            self._queue.get_nowait()
        self._queue.put_nowait(event)

    def __aiter__(
        self: EventStream,
    ) -> EventStream:
        """
        Return the stream as its own asynchronous iterator.
        """
        return self

    async def __anext__(
        self: EventStream,
    ) -> Event:
        """
        Wait for the next buffered event or signal stream exhaustion.
        """
        self._check_loop()
        if self._closed:
            raise StopAsyncIteration
        if self._reading:
            raise RuntimeError("EventStream permits only one consumer")
        self._reading = True
        try:
            event: Event | None = await self._queue.get()
            if event is None:
                raise StopAsyncIteration
            return event
        finally:
            self._reading = False

    async def aclose(
        self: EventStream,
    ) -> None:
        """
        Disconnect the stream and wake its pending consumer.
        """
        self._check_loop()
        if self._closed:
            return
        self._closed = True
        self._subscription.cancel()
        while not self._queue.empty():
            self._queue.get_nowait()
        self._queue.put_nowait(None)

    async def __aenter__(
        self: EventStream,
    ) -> EventStream:
        """
        Enter the asynchronous managed scope.
        """
        self._check_loop()
        if self._closed:
            raise RuntimeError("EventStream is closed")
        return self

    async def __aexit__(
        self: EventStream,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """
        Release resources when the asynchronous scope exits.

        Args:
            exc_type: Exception type leaving the managed scope, if any.
            exc: Exception leaving the managed scope, if any.
            tb: Traceback associated with the exception, if any.
        """
        await self.aclose()
