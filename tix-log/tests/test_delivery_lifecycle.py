"""
Regression coverage for producer barriers and interrupted resource cleanup.
"""

from __future__ import annotations

import asyncio
import io
import logging
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Literal

import pytest
from log import LogManager, QueueHandler, SinkHandler, StreamSink


@pytest.mark.parametrize("operation", ["flush", "close"])
@pytest.mark.parametrize("failure_type", [KeyboardInterrupt, SystemExit, asyncio.CancelledError])
def test_interrupted_cleanup_attempts_every_owned_handler(
    operation: Literal["flush", "close"],
    failure_type: type[BaseException],
) -> None:
    """
    Preserve interruptions while attempting every handler's remaining cleanup.

    Args:
        operation: First handler operation that raises an interruption.
        failure_type: Interruption propagated through the cleanup error group.
    """
    calls: list[tuple[logging.Handler, str]] = []
    failure: BaseException = failure_type("interrupted")
    pending: list[BaseException] = [failure]

    class Handler(
        logging.Handler,
    ):
        """
        Record cleanup and interrupt the selected operation once.
        """

        def flush(
            self: Handler,
        ) -> None:
            """
            Record flushing and raise the selected one-time interruption.
            """
            calls.append((self, "flush"))
            if self is first and operation == "flush" and pending:
                raise pending.pop()

        def close(
            self: Handler,
        ) -> None:
            """
            Record closure and raise the selected one-time interruption.
            """
            calls.append((self, "close"))
            super().close()
            if self is first and operation == "close" and pending:
                raise pending.pop()

    first: Handler = Handler()
    second: Handler = Handler()
    manager: LogManager = LogManager(handlers=[first, second], close_handlers=True)
    caught: pytest.ExceptionInfo[BaseExceptionGroup[BaseException]]
    with pytest.raises(BaseExceptionGroup) as caught:
        manager.close()
    assert caught.value.exceptions == (failure,)
    assert calls == [(first, "flush"), (first, "close"), (second, "flush"), (second, "close")]
    manager.close()
    assert len(calls) == 4
    with pytest.raises(RuntimeError, match="closed"):
        manager.get_logger().info("late")


def test_flush_waits_for_prior_producers_without_waiting_for_later_ones(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Establish a producer cutoff while leaving later and recursive logging runnable.

    Args:
        monkeypatch: Fixture exposing the point where flush begins waiting.
    """
    before_entered: threading.Event = threading.Event()
    after_entered: threading.Event = threading.Event()
    release_before: threading.Event = threading.Event()
    release_after: threading.Event = threading.Event()
    waiting: threading.Event = threading.Event()
    output: io.StringIO = io.StringIO()

    def processor(
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Block the before/after events and recursively publish from the former.

        Args:
            event: Structured event selecting its synchronization point.
        """
        if event["message"] == "before":
            before_entered.set()
            assert release_before.wait(3)
            manager.get_logger().info("nested")
        elif event["message"] == "after":
            after_entered.set()
            assert release_after.wait(3)
        return event

    manager: LogManager = LogManager(
        handlers=[SinkHandler(StreamSink(output))], processors=[processor], close_handlers=True
    )
    original_wait: Callable[[float | None], bool] = manager._delivery._condition.wait

    def observe_wait(
        timeout: float | None = None,
    ) -> bool:
        """
        Announce the actual condition wait without changing its synchronization.

        Args:
            timeout: Optional wait timeout forwarded to the condition.
        """
        waiting.set()
        return original_wait(timeout)

    monkeypatch.setattr(manager._delivery._condition, "wait", observe_wait)
    pool: ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as pool:
        before: Future[bool] = pool.submit(manager.get_logger().info, "before")
        try:
            assert before_entered.wait(1)
            flushed: Future[None] = pool.submit(manager.flush)
            assert waiting.wait(1)
            assert not flushed.done()
            after: Future[bool] = pool.submit(manager.get_logger().info, "after")
            assert after_entered.wait(1)
            release_before.set()
            assert before.result(2)
            flushed.result(2)
            assert not after.done()
            assert output.getvalue().splitlines() == ["nested", "before"]
        finally:
            release_before.set()
            release_after.set()
    manager.close()
    assert output.getvalue().splitlines() == ["nested", "before", "after"]


def test_concurrent_flushes_finish_before_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Keep close behind all accepted producers and flush operations.

    Args:
        monkeypatch: Fixture exposing the number of lifecycle waiters.
    """
    entered: threading.Event = threading.Event()
    release: threading.Event = threading.Event()
    waiting: threading.Semaphore = threading.Semaphore(0)

    def processor(
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Hold a producer until both flushes and closure have started.

        Args:
            event: Event returned unchanged after the test releases it.
        """
        entered.set()
        assert release.wait(3)
        return event

    manager: LogManager = LogManager(processors=[processor])
    original_wait: Callable[[float | None], bool] = manager._delivery._condition.wait

    def observe_wait(
        timeout: float | None = None,
    ) -> bool:
        """
        Announce a lifecycle wait before releasing its condition lock.

        Args:
            timeout: Optional wait timeout forwarded to the condition.
        """
        waiting.release()
        return original_wait(timeout)

    monkeypatch.setattr(manager._delivery._condition, "wait", observe_wait)
    pool: ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as pool:
        producer: Future[bool] = pool.submit(manager.get_logger().info, "before")
        try:
            assert entered.wait(1)
            first: Future[None] = pool.submit(manager.flush)
            assert waiting.acquire(timeout=1)
            second: Future[None] = pool.submit(manager.flush)
            assert waiting.acquire(timeout=1)
            closer: Future[None] = pool.submit(manager.close)
            assert waiting.acquire(timeout=1)
            future: Future[None]
            for future in (first, second, closer):
                assert not future.done()
        finally:
            release.set()
        assert producer.result(2)
        first.result(2)
        second.result(2)
        closer.result(2)


def test_nested_queue_is_rejected_without_starting_another_worker() -> None:
    """
    Reject nested queues before acquiring resources and leave the original usable.
    """
    inner: QueueHandler = QueueHandler(logging.NullHandler())
    threads: tuple[threading.Thread, ...] = tuple(threading.enumerate())
    try:
        with pytest.raises(ValueError, match="nested queues"):
            QueueHandler(inner)
        assert tuple(threading.enumerate()) == threads
        inner.handle(logging.makeLogRecord({"msg": "still usable", "levelno": logging.INFO}))
        inner.flush(timeout=1)
        assert inner.stats.accepted == 1
    finally:
        inner.close(timeout=1)
