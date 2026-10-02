from __future__ import annotations

import logging
import queue
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

import pytest
from log import BoundLogger, LogManager, QueueHandler, RateLimit, execution_context


class BlockingSink(
    logging.Handler,
):
    """
    Test sink exposing delivery and closure synchronization points.
    """

    def __init__(
        self: BlockingSink,
    ) -> None:
        """
        Initialize the instance and its owned state.
        """
        super().__init__()
        self.entered: threading.Event = threading.Event()
        self.release_emit: threading.Event = threading.Event()
        self.records: list[logging.LogRecord] = []
        self.closed_after: int | None = None

    def emit(
        self: BlockingSink,
        record: logging.LogRecord,
    ) -> None:
        """
        Deliver the record or event through the configured execution path.

        Args:
            record: Log record to render or deliver.
        """
        self.entered.set()
        assert self.release_emit.wait(3)
        self.records.append(record)

    def close(
        self: BlockingSink,
    ) -> None:
        """
        Finish owned work and release this resource.
        """
        self.closed_after = len(self.records)
        super().close()


def test_manager_close_waits_for_accepted_producer() -> None:
    """
    Verify manager close waits for accepted producer.
    """
    sink: BlockingSink = BlockingSink()
    manager: LogManager = LogManager(handlers=[sink], close_handlers=True)
    pool: ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as pool:
        producer: Future[bool] = pool.submit(manager.get_logger().info, "last")
        try:
            assert sink.entered.wait(1)
            close: Future[None] = pool.submit(manager.close)
            # Sink has not completed: close must not close it prematurely.
            assert sink.closed_after is None
        finally:
            sink.release_emit.set()
        producer.result(2)
        close.result(2)
    assert sink.closed_after == 1
    with pytest.raises(RuntimeError):
        manager.get_logger().info("late")


def test_close_from_processor_fails_without_poisoning_manager() -> None:
    """
    Verify close from processor fails without poisoning manager.
    """

    def close_from_processor(
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Attempt manager closure while its processor lease is active.

        Args:
            event: Structured event being processed.
        """
        manager.close()
        return event

    manager: LogManager = LogManager(processors=[close_from_processor])
    with pytest.raises(RuntimeError, match="own callback"):
        manager.get_logger().info("reentrant")
    manager.close()


def test_close_from_queue_sink_cannot_wait_on_producer_or_worker() -> None:
    """
    Verify close from queue sink cannot wait on producer or worker.
    """

    class Sink(
        logging.Handler,
    ):
        """
        Test sink exercising worker failure or lifecycle reentry.
        """

        def emit(
            self: Sink,
            record: logging.LogRecord,
        ) -> None:
            """
            Deliver the record or event through the configured execution path.

            Args:
                record: Log record to render or deliver.
            """
            manager.close()

    queued: QueueHandler = QueueHandler(Sink())
    manager: LogManager = LogManager(handlers=[queued], close_handlers=True)
    manager.get_logger().info("reentrant")
    queued.flush(timeout=1)
    assert isinstance(queued.last_error, RuntimeError)
    manager.close()


def test_queue_timeout_retry_and_nested_field_snapshot() -> None:
    """
    Verify queue timeout retry and nested field snapshot.
    """
    sink: BlockingSink = BlockingSink()
    queued: QueueHandler = QueueHandler(sink, capacity=1, overflow="block", enqueue_timeout=0.01)
    logger: BoundLogger = LogManager(handlers=[queued]).get_logger()
    try:
        value: dict[str, list[int]] = {"nested": [1]}
        logger.info("first", data=value)
        assert sink.entered.wait(1)
        value["nested"].append(2)
        logger.info("second")
        with pytest.raises(queue.Full):
            logger.info("third")
        with pytest.raises(TimeoutError):
            queued.flush(timeout=0.01)
        with pytest.raises(TimeoutError):
            queued.close(timeout=0.01)
        with pytest.raises(RuntimeError):
            logger.info("after close")
    finally:
        sink.release_emit.set()
        queued.close(timeout=2)
    assert sink.records[0].event["data"] == {"nested": [1]}  # type: ignore[attr-defined]
    assert queued.stats.accepted == 2
    assert queued.stats.dropped == 1


def test_queue_close_wakes_blocked_producer() -> None:
    """
    Verify queue close wakes blocked producer.
    """
    sink: BlockingSink = BlockingSink()
    queued: QueueHandler = QueueHandler(sink, capacity=1, overflow="block")
    logger: BoundLogger = LogManager(handlers=[queued]).get_logger()
    try:
        logger.info("first")
        assert sink.entered.wait(1)
        logger.info("second")
        pool: ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=1) as pool:
            blocked: Future[bool] = pool.submit(logger.info, "third")
            with pytest.raises(TimeoutError):
                queued.close(timeout=0.01)
            with pytest.raises(RuntimeError, match="closed"):
                blocked.result(1)
    finally:
        sink.release_emit.set()
        queued.close(timeout=2)


def test_fatal_worker_failure_releases_waiters() -> None:
    """
    Verify fatal worker failure releases waiters.
    """

    class Sink(
        logging.Handler,
    ):
        """
        Test sink exercising worker failure or lifecycle reentry.
        """

        def emit(
            self: Sink,
            record: logging.LogRecord,
        ) -> None:
            """
            Deliver the record or event through the configured execution path.

            Args:
                record: Log record to render or deliver.
            """
            raise SystemExit("fatal")

    queued: QueueHandler = QueueHandler(Sink())
    logger: BoundLogger = LogManager(handlers=[queued]).get_logger()
    logger.info("fatal")
    try:
        with pytest.raises(RuntimeError, match="worker failed"):
            queued.flush(timeout=1)
        assert isinstance(queued.last_error, SystemExit)
    finally:
        with pytest.raises(RuntimeError, match="worker failed"):
            queued.close(timeout=1)

        # logging.shutdown calls flush again; prevent a deliberate fatal test
        # handler from producing an interpreter-exit diagnostic.
        def ignore_cleanup(
            timeout: float | None = None,
        ) -> None:
            """
            Suppress interpreter-exit diagnostics from the deliberately failed test queue.

            Args:
                timeout: Unused argument matching queue cleanup methods.
            """

        queued.flush = ignore_cleanup  # type: ignore[method-assign]
        queued.close = ignore_cleanup  # type: ignore[method-assign]


def test_rate_limit_is_atomic_under_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify rate limit is atomic under threads.

    Args:
        monkeypatch: Pytest fixture for temporary attribute replacement.
    """
    monkeypatch.setattr("log.processors.time.monotonic", lambda: 100.0)
    limiter: RateLimit = RateLimit(1, burst=5)
    event: dict[str, str] = {"logger": "test", "message": "same"}
    pool: ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=8) as pool:
        results: list[dict[str, Any] | None] = list(pool.map(limiter, [event] * 100))
    assert sum(value is not None for value in results) == 5
    assert limiter.dropped == 95


def test_rate_limit_refill_eviction_and_execution_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verify rate limit refill eviction and execution context.

    Args:
        monkeypatch: Pytest fixture for temporary attribute replacement.
    """
    now: list[float] = [100.0]
    monkeypatch.setattr("log.processors.time.monotonic", lambda: now[0])
    limiter: RateLimit = RateLimit(2, capacity=1)
    event: dict[str, str] = {"logger": "test", "message": "same"}
    assert limiter(event) is event
    assert limiter(event) is None
    now[0] += 0.5
    assert limiter(event) is event
    limiter({**event, "message": "different"})
    assert limiter(event) is event
    enriched: dict[str, Any] = execution_context(event)
    assert enriched["thread_id"] == threading.get_ident()
    assert "thread_id" not in event


@pytest.mark.parametrize("timeout", [-1, float("nan"), float("inf")])
def test_queue_rejects_invalid_timeout(
    timeout: float,
) -> None:
    """
    Verify queue rejects invalid timeout.

    Args:
        timeout: Maximum wait in seconds, or None for no deadline.
    """
    with pytest.raises(ValueError):
        QueueHandler(logging.NullHandler(), enqueue_timeout=timeout)
