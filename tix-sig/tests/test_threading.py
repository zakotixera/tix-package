from __future__ import annotations

import asyncio
import gc
import threading
import weakref
from concurrent.futures import Future, ThreadPoolExecutor

import pytest
from sig import CancellationToken, Phase, ShutdownContext, ShutdownManager, ShutdownReport, State


def test_token_notifies_multiple_loops_from_another_thread() -> None:
    """
    Verify token notifies multiple loops from another thread.
    """
    token: CancellationToken = CancellationToken()
    barrier: threading.Barrier = threading.Barrier(5)

    def waiter() -> str:
        """
        Run a cancellation waiter on its own event loop.
        """

        async def run() -> str:
            """
            Execute the assigned synchronous work.
            """
            task: asyncio.Task[str] = asyncio.create_task(token.wait_async(timeout=2))
            await asyncio.sleep(0)
            barrier.wait(2)
            return await task

        return asyncio.run(run())

    pool: ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures: list[Future[str]] = [pool.submit(waiter) for _ in range(4)]
        barrier.wait(2)
        token.cancel("all loops")
        assert [future.result(3) for future in futures] == ["all loops"] * 4


async def test_token_timeout_and_cancel_remove_waiters() -> None:
    """
    Verify token timeout and cancel remove waiters.
    """
    token: CancellationToken = CancellationToken()
    with pytest.raises(TimeoutError):
        await token.wait_async(timeout=0)
    assert not token._waiters
    waiter: asyncio.Task[str] = asyncio.create_task(token.wait_async())
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert not token._waiters
    assert token.cancel("after removal")


def test_child_tokens_preserve_cause_and_do_not_retain_discarded_children() -> None:
    """
    Verify child tokens preserve cause and do not retain discarded children.
    """
    parent: CancellationToken = CancellationToken()
    child: CancellationToken = parent.child()
    grandchild: CancellationToken = child.child()
    independent: CancellationToken = parent.child()
    independent.cancel("local")
    discarded: CancellationToken = parent.child()
    ref: weakref.ReferenceType[CancellationToken] = weakref.ref(discarded)
    del discarded
    gc.collect()
    assert ref() is None
    assert not parent.cancelled
    parent.cancel("parent")
    assert child.reason == grandchild.reason == "parent"
    assert independent.reason == "local"
    assert parent.child().reason == "parent"


def test_deep_token_tree_avoids_python_recursion_limit() -> None:
    """
    Verify deep token tree avoids python recursion limit.
    """
    root: CancellationToken = CancellationToken()
    leaf: CancellationToken = root
    _: int
    for _ in range(1500):
        leaf = leaf.child()
    root.cancel("deep")
    assert leaf.reason == "deep"


def test_cancel_race_has_one_first_cause() -> None:
    """
    Verify cancel race has one first cause.
    """
    token: CancellationToken = CancellationToken()
    children: list[CancellationToken] = [token.child() for _ in range(20)]
    pool: ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=8) as pool:
        results: list[bool] = list(pool.map(lambda i: token.cancel(str(i)), range(40)))
    assert sum(results) == 1
    assert all(child.reason == token.reason for child in children)


def test_cleanup_dependencies_override_lifo_and_preserve_phases() -> None:
    """
    Verify cleanup dependencies override lifo and preserve phases.
    """
    manager: ShutdownManager = ShutdownManager()
    calls: list[str] = []
    manager.register("database", lambda ctx: calls.append("db"), after=["requests"])
    manager.register("requests", lambda ctx: calls.append("requests"), after=["stop"])
    manager.register("stop", lambda ctx: calls.append("stop"), phase=Phase.STOP)
    manager.register("cache", lambda ctx: calls.append("cache"), after=["database"])
    assert manager.planned_hooks == ("stop", "requests", "database", "cache")
    manager.shutdown().raise_errors()
    assert calls == ["stop", "requests", "db", "cache"]


@pytest.mark.parametrize("kind", ["missing", "cycle", "phase"])
def test_invalid_dependency_plan_does_not_start_shutdown(
    kind: str,
) -> None:
    """
    Verify invalid dependency plan does not start shutdown.

    Args:
        kind: Invalid dependency scenario exercised by the test.
    """
    manager: ShutdownManager = ShutdownManager()
    manager.register("a", lambda ctx: None, after=["b"], phase=Phase.STOP)
    if kind != "missing":
        manager.register(
            "b",
            lambda ctx: None,
            phase=Phase.CLOSE if kind == "phase" else Phase.STOP,
            after=["a"] if kind == "cycle" else [],
        )
    with pytest.raises(ValueError):
        manager.shutdown()
    assert manager.state is State.ACTIVE
    assert not manager.token.cancelled
    assert manager.unregister("a")


async def test_wait_closed_observes_report_and_rejects_self_wait() -> None:
    """
    Verify wait closed observes report and rejects self wait.
    """
    manager: ShutdownManager = ShutdownManager()
    observer: asyncio.Task[ShutdownReport] = asyncio.create_task(
        manager.wait_closed_async(timeout=1)
    )

    async def hook(
        ctx: ShutdownContext,
    ) -> None:
        """
        Exercise cleanup behavior using the supplied shutdown context.

        Args:
            ctx: Shutdown context supplied to the test or example hook.
        """
        with pytest.raises(RuntimeError, match="own completion"):
            await manager.wait_closed_async()
        with pytest.raises(RuntimeError, match="executing thread"):
            manager.wait_closed()

    manager.register("hook", hook)
    report: ShutdownReport = await manager.shutdown_async()
    report.raise_errors()
    assert await observer is report
    assert manager.wait_closed(0) is report


async def test_interrupted_report_includes_current_and_unattempted_hooks() -> None:
    """
    Verify interrupted report includes current and unattempted hooks.
    """
    manager: ShutdownManager = ShutdownManager()
    started: asyncio.Event = asyncio.Event()

    async def wait(
        ctx: ShutdownContext,
    ) -> None:
        """
        Wait synchronously for cancellation or deadline expiry.

        Args:
            ctx: Shutdown context supplied to the test or example hook.
        """
        started.set()
        await asyncio.Event().wait()

    manager.register("later", lambda ctx: None)
    manager.register("current", wait)
    task: asyncio.Task[ShutdownReport] = asyncio.create_task(manager.shutdown_async())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    report: ShutdownReport = await manager.wait_closed_async(timeout=1)
    assert report.interrupted
    assert [hook.name for hook in report.hooks] == ["current", "later"]
    assert not report.hooks[0].skipped
    assert report.hooks[1].skipped
    assert len(report.errors) == 2


async def test_programmatic_request_wakes_without_signal_poll_delay() -> None:
    """
    Verify programmatic request wakes without signal poll delay.
    """
    manager: ShutdownManager = ShutdownManager(poll_interval=30)
    task: asyncio.Task[str] = asyncio.create_task(manager.wait_async(timeout=1))
    await asyncio.sleep(0)
    await asyncio.to_thread(manager.request, "immediate")
    assert await task == "immediate"
