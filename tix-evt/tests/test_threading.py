from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future, ThreadPoolExecutor

import pytest
from evt import ANY, DispatchResult, Event, EventBus, EventStream, Subscription


async def test_wait_for_thread_delivery_filter_and_unsubscribe() -> None:
    """
    Verify wait for thread delivery filter and unsubscribe.
    """
    bus: EventBus = EventBus()
    task: asyncio.Task[Event] = asyncio.create_task(
        bus.wait_for("t", predicate=lambda event: event.payload == 2, timeout=1)
    )
    await asyncio.sleep(0)
    await asyncio.to_thread(bus.emit, "t", 1)
    assert not task.done()
    await asyncio.to_thread(bus.emit, "t", 2)
    assert (await task).payload == 2
    assert bus.listener_count == 0


async def test_wait_timeout_and_cancellation_disconnect() -> None:
    """
    Verify wait timeout and cancellation disconnect.
    """
    bus: EventBus = EventBus()
    with pytest.raises(TimeoutError):
        await bus.wait_for("t", timeout=0)
    assert bus.listener_count == 0
    task: asyncio.Task[Event] = asyncio.create_task(bus.wait_for("t"))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert bus.listener_count == 0


async def test_submit_marshals_to_stream_loop_and_propagates_result() -> None:
    """
    Verify submit marshals to stream loop and propagates result.
    """
    bus: EventBus = EventBus()
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    thread_id: int = threading.get_ident()
    bus.subscribe("t", lambda event: threading.get_ident())
    stream: EventStream
    async with EventStream(bus, "t") as stream:
        future: Future[DispatchResult] = await asyncio.to_thread(bus.submit, loop, "t", 42)
        result: DispatchResult = await asyncio.wrap_future(future)
        assert result.outcomes[0].value == thread_id
        assert (await anext(stream)).payload == 42


async def test_submit_cancellation_drains_handler() -> None:
    """
    Verify submit cancellation drains handler.
    """
    bus: EventBus = EventBus()
    started: asyncio.Event
    finished: asyncio.Event
    started, finished = asyncio.Event(), asyncio.Event()

    async def callback(
        event: Event,
    ) -> None:
        """
        Handle the notification used by this regression scenario.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            finished.set()

    bus.subscribe("t", callback)
    future: Future[DispatchResult] = await asyncio.to_thread(
        bus.submit, asyncio.get_running_loop(), "t"
    )
    await asyncio.wait_for(started.wait(), 1)
    future.cancel()
    await asyncio.wait_for(finished.wait(), 1)


def test_submit_rejects_stopped_loop_without_creating_coroutine() -> None:
    """
    Verify submit rejects stopped loop without creating coroutine.
    """
    loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    try:
        with pytest.raises(RuntimeError, match="running"):
            EventBus().submit(loop, "t")
    finally:
        loop.close()


def test_registry_concurrent_mutations_and_dispatch() -> None:
    """
    Verify registry concurrent mutations and dispatch.
    """
    bus: EventBus = EventBus()
    permanent: Subscription = bus.subscribe(ANY, lambda event: event.payload, priority=1)

    def run(
        index: int,
    ) -> None:
        """
        Execute the assigned synchronous work.

        Args:
            index: Identifier of the concurrent test publisher.
        """
        _: int
        for _ in range(50):
            with bus.subscribe("t", lambda event: event.payload):
                bus.emit("t", index)

    pool: ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(run, range(8)))
    assert bus.listener_count == 1
    assert bus.listeners("t") == (permanent.id,)
