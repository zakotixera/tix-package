# tix-evt

An instance-local event bus for Python 3.11+, with no runtime dependencies.
Install with `pip install ./tix-evt` from the repository root.

```python
from evt import ANY, EventBus

bus = EventBus()
with bus.subscribe("order.created", lambda event: event.payload["id"], priority=10):
    result = bus.emit("order.created", {"id": 42})
    assert result.outcomes[0].value == 42

audit = bus.subscribe(ANY, lambda event: print(event.topic), once=True)
bus.emit("order.shipped", 42)
```

Topics are arbitrary hashable objects; `ANY` is a subscription-only wildcard.
Handlers accept one immutable `Event(topic, payload, sender, metadata)` envelope.
Metadata is shallow-copied and read-only; nested values and payloads are not copied.
Higher priorities run first; ties follow registration order. Each subscription is
independent, even when callbacks are identical. `Subscription.cancel()` is
idempotent, and a handle is also a synchronous context manager.

`subscribe(topic, callback, priority=0, once=False, weak=False, sender=ANY,
predicate=None)` supports sender identity filtering, synchronous predicates and
weak functions/bound methods. Strong references are the default. Keep your own
reference to weak callbacks. Once subscriptions are atomically removed after a
predicate matches, before callback invocation: a failing callback still consumes
the subscription. Ordinary dispatch uses a snapshot; cancel/clear during dispatch
affects subsequent emissions, while already selected ordinary listeners may run.

`EventBus(errors="raise")` invokes all eligible handlers and then raises an
`ExceptionGroup` for ordinary failures. `errors="collect"` instead returns those
failures in `DispatchResult.outcomes`; call `raise_errors()` when desired. Predicate
failures are included too. `KeyboardInterrupt`, `SystemExit` and asyncio cancellation
propagate. A sync `emit()` encountering an awaitable closes a newly created coroutine
and records a TypeError; it never runs a hidden event loop. User-created Tasks remain
user-owned. `clear()` removes subscriptions; `listener_count` reports active entries.
`with bus.muted():` suppresses emissions only in the current context and its children.

```python
import asyncio
from evt import EventBus, EventStream


async def main():
    bus = EventBus()

    async def handle(event):
        await asyncio.sleep(0)
        return event.payload * 2

    bus.subscribe("work", handle)
    result = await bus.emit_async("work", 3, concurrency=4)
    assert result.outcomes[0].value == 6
    async with EventStream(bus, "updates", capacity=16) as stream:
        bus.emit("updates", "ready")
        assert (await anext(stream)).payload == "ready"


asyncio.run(main())
```

Async dispatch defaults to sequential execution. `concurrency=N` limits worker
tasks; result order remains subscription order while completion order can differ.
Cancellation cancels and awaits all workers before returning. Sync callbacks run
inline, so use an explicit `asyncio.to_thread` adapter for blocking work.

`EventStream` requires a running loop and one consumer. It defaults to capacity 128
and `overflow="drop_oldest"`; alternatives are `drop_newest` and `raise` (reported by
the bus's error policy). `dropped` counts overflow loss. `aclose()` disconnects,
discards buffered events and wakes the consumer. Streams must be used on their
owning loop, including publishing; marshal other threads with `call_soon_threadsafe`.
No hidden tasks, unbounded buffers, persistence, replay, retries or broker guarantees.

`await bus.wait_for(topic, predicate=None, sender=ANY, timeout=None)` waits for one
matching event and disconnects on completion, cancellation or timeout. Registration
occurs when the coroutine begins executing. Publishers may run in other threads;
delivery is marshalled to the waiter's loop. Predicates execute on the publisher.

`bus.submit(loop, topic, payload, sender=None, metadata=None, concurrency=1)` submits
async dispatch from another thread and returns a `concurrent.futures.Future`.
Cancellation forwards to dispatch. This also provides safe delivery to a stream's
owning loop. The caller must keep that loop running until submitted work settles;
never block on `future.result()` inside the target loop. Metadata is copied at
submission, while nested metadata and payload values remain caller-owned.

`listeners(topic)` returns a snapshot of matching subscription IDs, including `ANY`.
Merged exact-topic/wildcard snapshots are reused until a matching subscription changes.
The cache retains only topics with exact subscriptions; emitting arbitrary topics
does not grow it. Exact-topic mutations, one-shot claims, and weak callback expiry
invalidate only that topic; wildcard changes invalidate every cached topic. New
subscriptions use binary insertion into priority order, with registration order
breaking ties. In-flight dispatch retains its existing snapshot.
`EventStream` consumes the `EventSource` protocol, so custom sources can implement
the subscription contract without inheriting from `EventBus`. Registry state and
dispatch execution are separate components. Instances are process-local: inherited
buses/streams fail before acquiring parent locks. Build a fresh bus in each worker;
use an explicit IPC transport for cross-process events.
