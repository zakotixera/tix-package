from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import textwrap
import threading
from collections.abc import Callable
from types import FrameType
from typing import Any

import pytest
from sig import (
    CancellationToken,
    Phase,
    ShutdownContext,
    ShutdownManager,
    ShutdownReport,
    ShutdownRequested,
    SignalAdapter,
    State,
)


def test_token_first_reason_and_checkpoint() -> None:
    """
    Verify token first reason and checkpoint.
    """
    token: CancellationToken = CancellationToken()
    assert not token.wait(0)
    assert token.cancel("first")
    assert not token.cancel("second")
    assert token.wait(0)
    assert token.reason == "first"
    with pytest.raises(ShutdownRequested, match="first"):
        token.checkpoint()


async def test_token_waits_without_executor_and_is_cancellable() -> None:
    """
    Verify token waits without executor and is cancellable.
    """
    token: CancellationToken = CancellationToken()
    waiter: asyncio.Task[str] = asyncio.create_task(token.wait_async())
    await asyncio.sleep(0)
    token.cancel("done")
    assert await asyncio.wait_for(waiter, 1) == "done"
    waiter = asyncio.create_task(CancellationToken().wait_async())
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter


def test_shutdown_order_failure_isolation_and_idempotency() -> None:
    """
    Verify shutdown order failure isolation and idempotency.
    """
    manager: ShutdownManager = ShutdownManager()
    calls: list[str] = []
    manager.register("close-first", lambda ctx: calls.append("close-first"))
    manager.register("close-last", lambda ctx: calls.append("close-last"))
    manager.register("drain", lambda ctx: calls.append("drain"), phase=Phase.DRAIN)
    manager.register("priority", lambda ctx: calls.append("priority"), priority=10)

    def fail(
        ctx: ShutdownContext,
    ) -> None:
        """
        Raise the expected failure for the regression scenario.

        Args:
            ctx: Shutdown context supplied to the test or example hook.
        """
        calls.append("stop")
        assert ctx.token.cancelled
        assert ctx.reason == "test"
        raise ValueError("stop failed")

    manager.register("stop", fail, phase=Phase.STOP)
    assert manager.request("test")
    assert manager.state is State.REQUESTED
    assert not manager.request("again")
    assert manager.escalated
    report: ShutdownReport = manager.shutdown()
    assert calls == ["stop", "drain", "priority", "close-last", "close-first"]
    assert report.reason == "test"
    assert report.escalated
    assert manager.state is State.CLOSED
    assert manager.shutdown() is report
    assert not manager.request("late")
    with pytest.raises(ExceptionGroup):
        report.raise_errors()
    with pytest.raises(RuntimeError):
        manager.register("late", lambda ctx: None)


def test_registration_validation_and_reentrant_cleanup() -> None:
    """
    Verify registration validation and reentrant cleanup.
    """
    manager: ShutdownManager = ShutdownManager()
    manager.register("a", lambda ctx: manager.shutdown())
    with pytest.raises(ValueError):
        manager.register("a", lambda ctx: None)
    manager.register("b", lambda ctx: None)
    assert manager.unregister("b")
    assert not manager.unregister("b")
    report: ShutdownReport = manager.shutdown()
    assert isinstance(report.errors[0], RuntimeError)


def test_overlapping_shutdown_does_not_run_hooks_twice() -> None:
    """
    Verify overlapping shutdown does not run hooks twice.
    """
    manager: ShutdownManager = ShutdownManager()
    entered: threading.Event
    release: threading.Event
    entered, release = threading.Event(), threading.Event()

    def hook(
        ctx: ShutdownContext,
    ) -> None:
        """
        Exercise cleanup behavior using the supplied shutdown context.

        Args:
            ctx: Shutdown context supplied to the test or example hook.
        """
        entered.set()
        assert release.wait(3)

    manager.register("hook", hook)
    worker: threading.Thread = threading.Thread(target=manager.shutdown)
    worker.start()
    try:
        assert entered.wait(2)
        with pytest.raises(RuntimeError, match="already running"):
            manager.shutdown()
    finally:
        release.set()
        worker.join(3)
    assert not worker.is_alive()
    assert len(manager.shutdown().hooks) == 1


def test_sync_budget_reports_overrun_and_skips_remaining(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify sync budget reports overrun and skips remaining.

    Args:
        monkeypatch: Pytest fixture for temporary attribute replacement.
    """
    now: list[float] = [100.0]
    monkeypatch.setattr("sig._hooks.time.perf_counter", lambda: now[0])
    manager: ShutdownManager = ShutdownManager(timeout=0.001)
    manager.register("later", lambda ctx: pytest.fail("budget exhausted"))
    manager.register("slow", lambda ctx: now.__setitem__(0, now[0] + 0.01))
    report: ShutdownReport = manager.shutdown()
    assert isinstance(report.hooks[0].error, TimeoutError)
    assert report.hooks[1].skipped


async def test_async_hook_timeout_continues_and_global_budget_skips() -> None:
    """
    Verify async hook timeout continues and global budget skips.
    """
    manager: ShutdownManager = ShutdownManager(timeout=2)
    completed: list[int] = []

    async def slow(
        ctx: ShutdownContext,
    ) -> None:
        """
        Wait until the cleanup budget cancels this hook.

        Args:
            ctx: Shutdown context supplied to the test or example hook.
        """
        await asyncio.Event().wait()

    manager.register("later", lambda ctx: completed.append(1))
    manager.register("slow", slow, timeout=0.01)
    report: ShutdownReport = await manager.shutdown_async()
    assert isinstance(report.errors[0], TimeoutError)
    assert completed == [1]
    manager = ShutdownManager(timeout=0)
    manager.register("never", lambda ctx: pytest.fail("zero budget"))
    assert (await manager.shutdown_async()).hooks[0].skipped


async def test_external_cancellation_preserves_partial_report() -> None:
    """
    Verify external cancellation preserves partial report.
    """
    manager: ShutdownManager = ShutdownManager()
    entered: asyncio.Event = asyncio.Event()

    async def wait(
        ctx: ShutdownContext,
    ) -> None:
        """
        Wait synchronously for cancellation or deadline expiry.

        Args:
            ctx: Shutdown context supplied to the test or example hook.
        """
        entered.set()
        await asyncio.Event().wait()

    manager.register("waiting", wait)
    task: asyncio.Task[ShutdownReport] = asyncio.create_task(manager.shutdown_async())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert manager.state is State.CLOSED
    report: ShutdownReport = await manager.shutdown_async()
    assert report.interrupted


def test_sync_shutdown_rejects_coroutine_hook_cleanly() -> None:
    """
    Verify sync shutdown rejects coroutine hook cleanly.
    """

    async def hook(
        ctx: ShutdownContext,
    ) -> None:
        """
        Exercise cleanup behavior using the supplied shutdown context.

        Args:
            ctx: Shutdown context supplied to the test or example hook.
        """
        return None

    manager: ShutdownManager = ShutdownManager()
    manager.register("async", hook)
    assert isinstance(manager.shutdown().errors[0], TypeError)


def test_signal_adapter_defers_work_and_restores_handler() -> None:
    """
    Verify signal adapter defers work and restores handler.
    """
    previous: Callable[[int, FrameType | None], Any] | int | None = signal.getsignal(signal.SIGINT)
    manager: ShutdownManager = ShutdownManager()
    with manager.signals([signal.SIGINT]):
        signal.raise_signal(signal.SIGINT)
        assert not manager.token.cancelled
        assert manager.wait(1)
        assert manager.token.reason == "SIGINT"
        signal.raise_signal(signal.SIGINT)
        assert manager.escalated
    assert signal.getsignal(signal.SIGINT) == previous


def test_signal_ownership_rollback_and_foreign_replacement() -> None:
    """
    Verify signal ownership rollback and foreign replacement.
    """
    previous: Callable[[int, FrameType | None], Any] | int | None = signal.getsignal(signal.SIGINT)
    first: SignalAdapter = SignalAdapter(lambda reason: None, [signal.SIGINT])
    second: SignalAdapter = SignalAdapter(lambda reason: None, [signal.SIGINT])
    with first:
        with pytest.raises(RuntimeError):
            second.install()
    invalid: SignalAdapter = SignalAdapter(lambda reason: None, [signal.SIGINT, 999999])
    with pytest.raises((ValueError, OSError)):
        invalid.install()
    assert signal.getsignal(signal.SIGINT) == previous

    def foreign(
        number: int,
        frame: FrameType | None,
    ) -> None:
        """
        Stand in for a signal handler installed by another owner.

        Args:
            number: Operating-system signal number.
            frame: Interrupted stack frame, when supplied by the interpreter.
        """
        pass

    try:
        first.install()
        signal.signal(signal.SIGINT, foreign)
        first.close()
        assert signal.getsignal(signal.SIGINT) is foreign
    finally:
        signal.signal(signal.SIGINT, previous)


def test_signal_install_rejects_worker_thread() -> None:
    """
    Verify signal install rejects worker thread.
    """
    errors: list[RuntimeError] = []

    def worker() -> None:
        """
        Process queued work until the assigned input is exhausted.
        """
        error: RuntimeError
        try:
            SignalAdapter(lambda reason: None).install()
        except RuntimeError as error:
            errors.append(error)

    thread: threading.Thread = threading.Thread(target=worker)
    thread.start()
    thread.join(2)
    assert len(errors) == 1


async def test_async_manager_wait_drains_signals() -> None:
    """
    Verify async manager wait drains signals.
    """
    manager: ShutdownManager = ShutdownManager(poll_interval=0.001)
    with manager.signals([signal.SIGINT]):
        waiter: asyncio.Task[str] = asyncio.create_task(manager.wait_async())
        signal.raise_signal(signal.SIGINT)
        assert await asyncio.wait_for(waiter, 1) == "SIGINT"


def test_signal_delivery_in_subprocess() -> None:
    """
    Verify signal delivery in subprocess.
    """
    code: str = textwrap.dedent("""
        import os, signal
        from sig import ShutdownManager
        manager = ShutdownManager()
        manager.register('cleanup', lambda ctx: print('cleaned:' + ctx.reason))
        with manager.signals([signal.SIGINT]):
            if os.name == 'nt':
                signal.raise_signal(signal.SIGINT)
            else:
                os.kill(os.getpid(), signal.SIGINT)
            assert manager.wait(2)
            manager.shutdown().raise_errors()
    """)
    result: subprocess.CompletedProcess[str] = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=10,
        env=os.environ.copy(),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "cleaned:SIGINT"


async def test_empty_reason_is_preserved_everywhere() -> None:
    """
    Verify empty reason is preserved everywhere.
    """
    manager: ShutdownManager = ShutdownManager()
    manager.request("")
    assert await manager.wait_async() == ""
    assert await manager.token.wait_async() == ""
    manager.register("reason", lambda ctx: None if ctx.reason == "" else pytest.fail("reason lost"))
    report: ShutdownReport = await manager.shutdown_async()
    report.raise_errors()
    assert report.reason == ""


@pytest.mark.parametrize("options", [{"timeout": -1}, {"poll_interval": 0}])
def test_shutdown_configuration_validation(
    options: dict[str, Any],
) -> None:
    """
    Verify shutdown configuration validation.

    Args:
        options: Configuration values exercised by the parametrized test.
    """
    with pytest.raises(ValueError):
        ShutdownManager(**options)


def test_wait_times_out_without_request() -> None:
    """
    Verify wait times out without request.
    """
    manager: ShutdownManager = ShutdownManager()
    assert not manager.wait(0)
    assert manager.state is State.ACTIVE


def test_base_exception_closes_shutdown_lifecycle() -> None:
    """
    Verify base exception closes shutdown lifecycle.
    """
    manager: ShutdownManager = ShutdownManager()

    def interrupt(
        ctx: ShutdownContext,
    ) -> None:
        """
        Interrupt cleanup to verify final lifecycle state.

        Args:
            ctx: Shutdown context supplied to the test or example hook.
        """
        raise KeyboardInterrupt

    manager.register("interrupt", interrupt)
    with pytest.raises(KeyboardInterrupt):
        manager.shutdown()
    assert manager.state is State.CLOSED
    assert manager.shutdown().interrupted
