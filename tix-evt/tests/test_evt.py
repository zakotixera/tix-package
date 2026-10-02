from __future__ import annotations

import asyncio
import gc
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Literal

import pytest
from evt import ANY, DispatchResult, Event, EventBus, EventStream, Subscription


def test_priority_wildcard_sender_predicate_and_outcomes() -> None:
    """
    Verify priority wildcard sender predicate and outcomes.
    """
    bus: EventBus = EventBus()
    sender: object = object()
    bus.subscribe("work", lambda event: "low", priority=-1)
    bus.subscribe(ANY, lambda event: "all", priority=1)
    bus.subscribe(
        "work", lambda event: "selected", sender=sender, predicate=lambda event: event.payload > 0
    )
    result: DispatchResult = bus.emit("work", 1, sender=sender)
    assert [item.value for item in result.outcomes] == ["all", "selected", "low"]
    result = bus.emit("work", -1, sender=sender)
    assert [item.value for item in result.outcomes] == ["all", "low"]
    with pytest.raises(ValueError):
        bus.emit(ANY)


def test_snapshot_mutation_and_subscription_context() -> None:
    """
    Verify snapshot mutation and subscription context.
    """
    bus: EventBus = EventBus()
    calls: list[str] = []

    def first(
        event: Event,
    ) -> None:
        """
        Mutate subscriptions during dispatch to exercise snapshot behavior.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        second.cancel()
        bus.subscribe("topic", lambda event: calls.append("new"))

    bus.subscribe("topic", first)
    second: Subscription = bus.subscribe("topic", lambda event: calls.append("second"))
    bus.emit("topic")
    assert calls == ["second"]
    bus.emit("topic")
    assert calls == ["second", "new"]
    count: int = bus.listener_count
    with bus.subscribe("other", lambda event: None):
        assert bus.listener_count == count + 1
    assert bus.listener_count == count


def test_once_reentrant_and_concurrent_dispatch() -> None:
    """
    Verify once reentrant and concurrent dispatch.
    """
    bus: EventBus = EventBus()
    calls: list[Any] = []

    def callback(
        event: Event,
    ) -> None:
        """
        Handle the notification used by this regression scenario.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        calls.append(event.payload)
        bus.emit("work", "reentrant")

    bus.subscribe("work", callback, once=True)
    barrier: threading.Barrier = threading.Barrier(8)

    def publish(
        index: int,
    ) -> None:
        """
        Emit the test event after synchronizing concurrent publishers.

        Args:
            index: Identifier of the concurrent test publisher.
        """
        barrier.wait(3)
        bus.emit("work", index)

    pool: ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(publish, range(8)))
    assert len(calls) == 1
    assert bus.listener_count == 0


def test_once_is_not_consumed_by_predicate_miss() -> None:
    """
    Verify once is not consumed by predicate miss.
    """
    bus: EventBus = EventBus()
    bus.subscribe(
        "t", lambda event: event.payload, once=True, predicate=lambda event: event.payload == 2
    )
    assert not bus.emit("t", 1).outcomes
    assert bus.listener_count == 1
    assert bus.emit("t", 2).outcomes[0].value == 2
    assert bus.listener_count == 0


def test_weak_method_does_not_retain_owner_or_bus() -> None:
    """
    Verify weak method does not retain owner or bus.
    """
    bus: EventBus = EventBus()

    class Receiver:
        """
        Test receiver whose lifetime controls a weak subscription.
        """

        def receive(
            self: Receiver,
            event: Event,
        ) -> int:
            """
            Forward a matching event to the waiting consumer.

            Args:
                event: Event being transformed, selected, or delivered.
            """
            return 1

    receiver: Receiver = Receiver()
    ref: weakref.ReferenceType[Receiver] = weakref.ref(receiver)
    handle: Subscription = bus.subscribe("work", receiver.receive, weak=True)
    assert bus.emit("work").outcomes[0].value == 1
    del receiver
    gc.collect()
    assert ref() is None
    assert bus.listener_count == 0
    assert not handle.cancel()
    bus_ref: weakref.ReferenceType[EventBus] = weakref.ref(bus)
    del bus
    gc.collect()
    assert bus_ref() is None


@pytest.mark.parametrize("policy", ["collect", "raise"])
def test_error_policy_runs_later_handlers(
    policy: Literal["collect", "raise"],
) -> None:
    """
    Verify error policy runs later handlers.

    Args:
        policy: Delivery policy selected by the parametrized test.
    """
    bus: EventBus = EventBus(errors=policy)
    calls: list[int] = []

    def fail(
        event: Event,
    ) -> None:
        """
        Raise the expected failure for the regression scenario.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        raise ValueError("bad")

    bus.subscribe("t", fail)
    bus.subscribe("t", lambda event: calls.append(1))
    if policy == "raise":
        with pytest.raises(ExceptionGroup):
            bus.emit("t")
    else:
        result: DispatchResult = bus.emit("t")
        assert isinstance(result.errors[0], ValueError)
    assert calls == [1]


def test_sync_rejects_async_without_unawaited_coroutine() -> None:
    """
    Verify sync rejects async without unawaited coroutine.
    """
    bus: EventBus = EventBus(errors="collect")

    async def handler(
        event: Event,
    ) -> int:
        """
        Handle the event used by this regression scenario.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        return 1

    bus.subscribe("t", handler)
    assert isinstance(bus.emit("t").errors[0], TypeError)


async def test_async_concurrency_limit_and_result_order() -> None:
    """
    Verify async concurrency limit and result order.
    """
    bus: EventBus = EventBus()
    running = peak = 0

    async def handler(
        event: Event,
    ) -> Any:
        """
        Handle the event used by this regression scenario.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.001)
        running -= 1
        return event.payload

    handles: list[Subscription] = [bus.subscribe("t", handler) for _ in range(10)]
    result: DispatchResult = await bus.emit_async("t", 5, concurrency=3)
    assert peak == 3
    assert [outcome.subscription_id for outcome in result.outcomes] == [h.id for h in handles]
    assert all(outcome.value == 5 for outcome in result.outcomes)


async def test_cancelled_dispatch_awaits_every_worker() -> None:
    """
    Verify cancelled dispatch awaits every worker.
    """
    bus: EventBus = EventBus()
    started: asyncio.Event = asyncio.Event()
    active: set[asyncio.Task[Any] | None] = set()

    async def handler(
        event: Event,
    ) -> None:
        """
        Handle the event used by this regression scenario.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        task: asyncio.Task[Any] | None = asyncio.current_task()
        active.add(task)
        if len(active) == 3:
            started.set()
        try:
            await asyncio.Event().wait()
        finally:
            active.remove(task)

    _: int
    for _ in range(10):
        bus.subscribe("t", handler)
    dispatch: asyncio.Task[DispatchResult] = asyncio.create_task(bus.emit_async("t", concurrency=3))
    await asyncio.wait_for(started.wait(), 2)
    dispatch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dispatch
    assert not active


async def test_muting_is_context_local() -> None:
    """
    Verify muting is context local.
    """
    bus: EventBus = EventBus()
    calls: list[Any] = []
    bus.subscribe("t", lambda event: calls.append(event.payload))

    async def muted() -> None:
        """
        Emit within a muted task-local scope.
        """
        with bus.muted():
            await asyncio.sleep(0)
            bus.emit("t", "muted")

    async def audible() -> None:
        """
        Emit from an independent, unmuted task context.
        """
        await asyncio.sleep(0)
        bus.emit("t", "audible")

    await asyncio.gather(muted(), audible())
    assert calls == ["audible"]


@pytest.mark.parametrize("policy,expected", [("drop_oldest", 2), ("drop_newest", 1), ("raise", 1)])
async def test_stream_overflow_policies(
    policy: Literal["drop_oldest", "drop_newest", "raise"],
    expected: int,
) -> None:
    """
    Verify stream overflow policies.

    Args:
        policy: Delivery policy selected by the parametrized test.
        expected: Expected result for the selected test case.
    """
    bus: EventBus = EventBus(errors="collect")
    stream: EventStream
    async with EventStream(bus, "t", capacity=1, overflow=policy) as stream:
        bus.emit("t", 1)
        result: DispatchResult = bus.emit("t", 2)
        assert stream.dropped == 1
        assert (await anext(stream)).payload == expected
        assert bool(result.errors) == (policy == "raise")
    assert bus.listener_count == 0


async def test_stream_close_wakes_pending_consumer() -> None:
    """
    Verify stream close wakes pending consumer.
    """
    bus: EventBus = EventBus()
    stream: EventStream = EventStream(bus, "t")
    pending: asyncio.Task[Event] = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    await stream.aclose()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(pending, 1)
    await stream.aclose()


def test_envelope_copies_metadata() -> None:
    """
    Verify envelope copies metadata.
    """
    metadata: dict[str, int] = {"id": 1}
    event: Event = Event("t", metadata=metadata)
    metadata["id"] = 2
    assert event.metadata["id"] == 1
    with pytest.raises(TypeError):
        event.metadata["id"] = 3  # type: ignore[index]  # Exercise the read-only runtime contract.


async def test_concurrent_emissions_claim_once_before_first_await() -> None:
    """
    Verify concurrent emissions claim once before first await.
    """
    bus: EventBus = EventBus()
    calls: list[Any] = []

    async def handle(
        event: Event,
    ) -> Any:
        """
        Apply record filters before submitting the accepted record.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        calls.append(event.payload)
        await asyncio.sleep(0)
        return event.payload

    bus.subscribe("t", handle, once=True)
    results: list[DispatchResult] = await asyncio.gather(
        *(bus.emit_async("t", i, concurrency=2) for i in range(10))
    )
    assert calls == [0]
    assert sum(len(result.outcomes) for result in results) == 1


async def test_async_failures_drain_other_handlers() -> None:
    """
    Verify async failures drain other handlers.
    """
    bus: EventBus = EventBus()
    calls: list[int] = []

    async def fail(
        event: Event,
    ) -> None:
        """
        Raise the expected failure for the regression scenario.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        raise ValueError("failed")

    async def complete(
        event: Event,
    ) -> None:
        """
        Complete the callback after yielding to other dispatch workers.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        await asyncio.sleep(0)
        calls.append(1)

    bus.subscribe("t", fail)
    bus.subscribe("t", complete)
    with pytest.raises(ExceptionGroup):
        await bus.emit_async("t", concurrency=2)
    assert calls == [1]


async def test_stream_rejects_foreign_thread_and_multiple_consumers() -> None:
    """
    Verify stream rejects foreign thread and multiple consumers.
    """
    bus: EventBus = EventBus(errors="collect")
    stream: EventStream
    async with EventStream(bus, "t") as stream:
        result: DispatchResult = await asyncio.to_thread(bus.emit, "t")
        assert isinstance(result.errors[0], RuntimeError)
        reader: asyncio.Task[Event] = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="one consumer"):
            await anext(stream)
        bus.emit("t", "valid")
        assert (await reader).payload == "valid"


def test_predicate_failure_retains_once_subscription() -> None:
    """
    Verify predicate failure retains once subscription.
    """
    bus: EventBus = EventBus(errors="collect")

    def predicate(
        event: Event,
    ) -> bool:
        """
        Raise the predicate failure exercised by this regression.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        raise ValueError("predicate")

    bus.subscribe("t", lambda event: None, once=True, predicate=predicate)
    assert isinstance(bus.emit("t").errors[0], ValueError)
    assert bus.listener_count == 1


def test_base_exception_propagates() -> None:
    """
    Verify base exception propagates.
    """
    bus: EventBus = EventBus()

    def stop(
        event: Event,
    ) -> None:
        """
        Interrupt the callback to verify BaseException propagation.

        Args:
            event: Event being transformed, selected, or delivered.
        """
        raise KeyboardInterrupt

    bus.subscribe("t", stop)
    with pytest.raises(KeyboardInterrupt):
        bus.emit("t")
