# tix-sig

Cooperative shutdown for Python 3.11+, with no runtime dependencies.
Install with `pip install ./tix-sig` from the repository root.

```python
from sig import Phase, ShutdownManager

manager = ShutdownManager(timeout=10)
manager.register("stop accepting", lambda ctx: print("stop"), phase=Phase.STOP)
manager.register("drain", lambda ctx: print("drain"), phase=Phase.DRAIN)
manager.register("close", lambda ctx: print(ctx.reason), phase=Phase.CLOSE)
manager.request("application finished")
report = manager.shutdown()
report.raise_errors()
```

`CancellationToken` is independently usable. `cancel(reason)` preserves the first
cause and returns whether it changed state. Inspect `cancelled`/`reason`, call
`checkpoint()` to raise `ShutdownRequested`, or use `wait(timeout)` and
`await wait_async(timeout=None)`. Async waiters are notified with their loop's
thread-safe scheduler; there is no token polling or executor thread. One token may
have waiters on several loops, and timeout/cancellation removes each waiter.
`child()` creates a token that inherits parent cancellation and preserves a child's
earlier local cause. Child cancellation never cancels the parent; discarded child
tokens are not retained by parents. A manager exposes its token for downstream
workers; request through `manager.request()` when using its lifecycle and escalation.

`ShutdownManager(timeout=None, poll_interval=0.02)` starts ACTIVE, becomes REQUESTED,
then RUNNING during cleanup and CLOSED when complete. `request(reason)` is safe from
ordinary threads. Repeated requests set `escalated` while keeping the original reason.
They never force an exit or interrupt a hook. Applications decide how to escalate.

Hooks receive `ShutdownContext(token, reason, deadline)` and can inspect monotonic
`remaining` time. Order is ascending `phase`, descending `priority`, then reverse
registration order. Use custom integer phases if needed. Names must be unique;
`after=["hook-name", ...]` declares cleanup ordering dependencies. Dependencies
override priority/LIFO within a phase; later-phase dependencies, missing names and
cycles raise before cleanup starts. Dependencies impose order, not success: failure
does not skip downstream cleanup. `planned_hooks` previews and validates the order.
`unregister(name)` works until cleanup starts. `register(..., timeout=seconds)` adds
a per-hook budget. Registration freezes at cleanup start. Failures are reported while
subsequent hooks run; `ShutdownReport.raise_errors()` raises an ExceptionGroup.
Repeated cleanup returns the same report; overlapping/reentrant cleanup raises.

`shutdown()` accepts only synchronous hooks. Its deadlines are checked between hooks
and report overruns; they cannot interrupt blocked Python functions. Async cleanup
uses `await manager.shutdown_async()` and awaits returned awaitables with per-hook
and total cooperative timeouts. Sync hooks still run inline. Cancellation propagates,
closes the lifecycle, and caches an `interrupted=True` report identifying the
interrupted hook and every unattempted hook; it does not
retry unfinished hooks. Cooperative timeouts cannot kill cancellation-resistant
coroutines. Skipped hooks are explicitly reported when the total budget is exhausted.

`wait_closed(timeout=None)` and `await wait_closed_async(timeout=None)` observe the
cached report without starting cleanup. Waiting from the cleanup's own thread/task
raises to prevent self-deadlock. `wait_async(timeout=None)` waits for a request;
programmatic requests wake immediately, while a timer polls pending OS signals at
the configured interval. All timeout values must be finite and nonnegative.
Absolute `ShutdownContext.deadline` values use `time.perf_counter()`; prefer
`context.remaining` when integrating other clocks or event loops.

```python
import asyncio
from sig import ShutdownManager


async def serve():
    manager = ShutdownManager(timeout=5)
    manager.register("release resources", lambda ctx: print(ctx.reason))
    with manager.signals():  # SIGINT/SIGTERM; install and restore in the main thread
        try:
            await manager.wait_async()
        finally:
            report = await manager.shutdown_async()
    report.raise_errors()


asyncio.run(serve())
```

OS integration is optional. `signals(iterable)` returns a `SignalAdapter`; its
context manager installs and restores handlers. Direct users can call `install()`
and `close()`. Partial installs roll back; overlapping adapters fail; restoration
does not overwrite handlers subsequently installed by someone else. Applications
must choose one signal owner when a framework already manages process signals.
The manager tracks adapters weakly. Installed OS handlers keep their adapter alive;
after closing it and dropping application references, the adapter can be collected
and disappears from manager polling. Close restores handlers before delivering any
pending notifications, so leaving the scope does not lose a shutdown request.
Retained adapters may be installed again.

The Python signal handler only appends a number. `poll()`, `state`, `escalated`,
`wait(timeout)`, `wait_async()` and cleanup drain pending requests outside signal
context. Reading the token alone does not drain OS signals. The polling interval
sets notification latency without occupying executor threads or taking locks in a
signal handler. Waiting on a token directly is for programmatic cancellation.

Windows supports only its catchable signals; configure `SIGBREAK` explicitly if
needed. Uncatchable signals, forcible termination and long native calls cannot
guarantee graceful cleanup. There is no implicit `atexit` hook, process exit,
background signal-monitor thread, or global asyncio/wakeup-fd modification.

Signal bursts are coalesced into at most two pending notifications (first cause and
escalation); polling is serialized across threads and drains a bounded batch. On
POSIX, explicit signal installation registers a child-after-fork hook that restores
owned handlers and clears inherited ownership without acquiring parent locks.
Managers and tokens reject inherited use; recreate them in the child. Only explicit
IPC such as a caller-owned `multiprocessing.Event` propagates cancellation between
processes. See [concurrency contracts](../docs/concurrency.md).

Cancellation groups a token's async waiters by their owning loop and schedules one
notification per loop. Cancelled futures are skipped during delivery. Waiting on an
already-cancelled token returns its first cause without allocating a future. The
same first-cause, timeout, linked-token, and cross-thread contracts apply.
