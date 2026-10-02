from __future__ import annotations

import asyncio
import io
import json
import logging
import queue
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Literal

import pytest
from log import BoundLogger, JsonFormatter, LogManager, QueueHandler, redact


def capture(
    **kwargs: Any,
) -> tuple[LogManager, io.StringIO]:
    """
    Create an isolated logging manager and its in-memory output stream.

    Args:
        kwargs: Keyword options forwarded to the logging manager.
    """
    output: io.StringIO = io.StringIO()
    handler: logging.StreamHandler[io.StringIO] = logging.StreamHandler(output)
    handler.setFormatter(JsonFormatter())
    return LogManager("test", handlers=[handler], **kwargs), output


def test_context_binding_precedence_restoration_and_lazy_filtering() -> None:
    """
    Verify context binding precedence restoration and lazy filtering.
    """
    manager: LogManager
    output: io.StringIO
    manager, output = capture()
    log: BoundLogger = manager.get_logger(user="bound")
    assert not log.debug(lambda: pytest.fail("disabled message evaluated"))
    with manager.context(user="outer"):
        with manager.context(user="inner"):
            log.info("first", user="call")
        log.info("second")
    log.info("third")
    assert [json.loads(line)["user"] for line in output.getvalue().splitlines()] == [
        "call",
        "outer",
        "bound",
    ]
    manager.set_level(logging.DEBUG)
    assert log.debug("enabled")
    manager.close()
    manager.close()
    with pytest.raises(RuntimeError):
        log.info("closed")


async def test_context_isolation_across_tasks() -> None:
    """
    Verify context isolation across tasks.
    """
    manager: LogManager
    output: io.StringIO
    manager, output = capture()
    log: BoundLogger = manager.get_logger()

    async def run(
        name: str,
    ) -> None:
        """
        Execute the assigned synchronous work.

        Args:
            name: Name identifying this instance or registered resource.
        """
        with manager.context(task=name):
            await asyncio.sleep(0)
            log.info("event")

    await asyncio.gather(run("one"), run("two"))
    assert [json.loads(line)["task"] for line in output.getvalue().splitlines()] == ["one", "two"]


def test_processors_drop_redact_and_exception_formatting() -> None:
    """
    Verify processors drop redact and exception formatting.
    """
    manager: LogManager
    output: io.StringIO
    manager, output = capture(
        processors=[
            redact(["password"]),
            lambda event: None if event["message"] == "drop" else event,
        ]
    )
    log: BoundLogger = manager.get_logger()
    value: dict[str, Any] = {
        "Password": "secret",
        "items": [{"password": "also secret"}],
    }
    assert not log.info("drop")
    log.info("keep", data=value)
    try:
        raise ValueError("bad input")
    except ValueError:
        log.exception("failed")
    events: list[Any] = [json.loads(line) for line in output.getvalue().splitlines()]
    assert events[0]["data"]["Password"] == "[REDACTED]"
    assert events[0]["data"]["items"][0]["password"] == "[REDACTED]"
    assert value["Password"] == "secret"
    assert "ValueError: bad input" in events[1]["exception"]


def test_no_global_logger_configuration_or_last_resort(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Verify no global logger configuration or last resort.

    Args:
        capsys: Pytest fixture capturing standard output and error.
    """
    root: logging.Logger = logging.getLogger()
    original: list[logging.Handler] = root.handlers[:]
    registry: dict[str, logging.Logger | logging.PlaceHolder] = dict(
        logging.Logger.manager.loggerDict
    )
    manager: LogManager = LogManager("not-registered")
    manager.get_logger().error("no output")
    assert root.handlers == original
    assert logging.Logger.manager.loggerDict == registry
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("field", ["level", "logger", "timestamp", "exception"])
def test_reserved_fields(
    field: str,
) -> None:
    """
    Verify reserved fields.

    Args:
        field: Reserved field name exercised by the test.
    """
    manager: LogManager = LogManager()
    with pytest.raises(ValueError):
        manager.get_logger(**{field: "oops"})


class ControlledSink(
    logging.Handler,
):
    """
    Test sink that waits for explicit release and optionally fails.
    """

    def __init__(
        self: ControlledSink,
        *,
        fail: bool = False,
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            fail: Whether the test sink raises instead of storing a record.
        """
        super().__init__()
        self.entered: threading.Event = threading.Event()
        self.allow_emit: threading.Event = threading.Event()
        self.records: list[logging.LogRecord] = []
        self.fail: bool = fail

    def emit(
        self: ControlledSink,
        record: logging.LogRecord,
    ) -> None:
        """
        Deliver the record or event through the configured execution path.

        Args:
            record: Log record to render or deliver.
        """
        self.entered.set()
        if not self.allow_emit.wait(5):
            raise TimeoutError("test sink was not released")
        if self.fail:
            raise ValueError("broken sink")
        self.records.append(record)


@pytest.mark.parametrize("policy", ["drop", "raise"])
def test_queue_pressure_context_capture_and_drain(
    policy: Literal["drop", "raise"],
) -> None:
    """
    Verify queue pressure context capture and drain.

    Args:
        policy: Delivery policy selected by the parametrized test.
    """
    sink: ControlledSink = ControlledSink()
    handler: QueueHandler = QueueHandler(sink, capacity=1, overflow=policy)
    manager: LogManager = LogManager(handlers=[handler], close_handlers=True)
    log: BoundLogger = manager.get_logger()
    try:
        with manager.context(request="producer"):
            log.info("first")
        assert sink.entered.wait(2)
        log.info("second")
        if policy == "raise":
            with pytest.raises(queue.Full):
                log.info("third")
        else:
            log.info("third")
        assert handler.stats.dropped == 1
    finally:
        sink.allow_emit.set()
        manager.close()
    assert [r.getMessage() for r in sink.records] == ["first", "second"]
    assert sink.records[0].event["request"] == "producer"  # type: ignore[attr-defined]
    assert handler.stats.accepted == 2
    handler.close()


def test_queue_block_policy_preserves_all_records() -> None:
    """
    Verify queue block policy preserves all records.
    """
    output: io.StringIO = io.StringIO()
    sink: logging.StreamHandler[io.StringIO] = logging.StreamHandler(output)
    handler: QueueHandler = QueueHandler(sink, capacity=2, overflow="block")
    manager: LogManager = LogManager(handlers=[handler], close_handlers=True)
    with manager:
        pool: ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda i: manager.get_logger().info(str(i)), range(100)))
    assert len(output.getvalue().splitlines()) == 100
    assert handler.stats.dropped == 0


def test_queue_worker_survives_sink_failure() -> None:
    """
    Verify queue worker survives sink failure.
    """
    sink: ControlledSink = ControlledSink(fail=True)
    sink.allow_emit.set()
    handler: QueueHandler = QueueHandler(sink)
    try:
        log: BoundLogger = LogManager(handlers=[handler]).get_logger()
        log.info("one")
        log.info("two")
        handler.flush()
        assert handler.stats.failed == 2
        assert isinstance(handler.last_error, ValueError)
    finally:
        handler.close()


def test_queue_reentrancy_is_reported_without_deadlock() -> None:
    """
    Verify queue reentrancy is reported without deadlock.
    """

    class ReentrantSink(
        logging.Handler,
    ):
        """
        Test sink that attempts to log back into its own queue.
        """

        def emit(
            self: ReentrantSink,
            record: logging.LogRecord,
        ) -> None:
            """
            Deliver the record or event through the configured execution path.

            Args:
                record: Log record to render or deliver.
            """
            log.info("recursive")

    handler: QueueHandler = QueueHandler(ReentrantSink(), capacity=1, overflow="block")
    log: BoundLogger = LogManager(handlers=[handler]).get_logger()
    try:
        log.info("start")
        handler.flush()
        assert isinstance(handler.last_error, RuntimeError)
    finally:
        handler.close()


def test_handler_ownership_and_cleanup_failure_isolation() -> None:
    """
    Verify handler ownership and cleanup failure isolation.
    """
    closed: list[Handler] = []

    class Handler(
        logging.Handler,
    ):
        """
        Test handler recording cleanup attempts and configured failures.
        """

        def flush(
            self: Handler,
        ) -> None:
            """
            Wait for previously accepted output to reach the sink.
            """
            raise ValueError("flush failed")

        def close(
            self: Handler,
        ) -> None:
            """
            Finish owned work and release this resource.
            """
            closed.append(self)
            super().close()

    first: Handler
    second: Handler
    first, second = Handler(), Handler()
    manager: LogManager = LogManager(handlers=[first, second], close_handlers=True)
    caught: pytest.ExceptionInfo[ExceptionGroup[Any]]
    with pytest.raises(ExceptionGroup) as caught:
        manager.close()
    assert len(caught.value.exceptions) == 2
    assert closed == [first, second]
    # Avoid intentionally failing test handlers at logging's interpreter shutdown.
    first.flush = lambda: None  # type: ignore[method-assign]
    second.flush = lambda: None  # type: ignore[method-assign]


def test_borrowed_handlers_and_filters_are_respected() -> None:
    """
    Verify borrowed handlers and filters are respected.
    """
    output: io.StringIO = io.StringIO()
    handler: logging.StreamHandler[io.StringIO] = logging.StreamHandler(output)
    handler.setLevel(logging.WARNING)
    handler.addFilter(lambda record: record.getMessage() != "filtered")
    manager: LogManager
    with LogManager(handlers=[handler]) as manager:
        log: BoundLogger = manager.get_logger().bind(component="a").unbind("component")
        log.info("below sink level")
        log.warning("filtered")
        log.error("accepted")
    assert output.getvalue().strip() == "accepted"
    assert not handler._closed  # type: ignore[attr-defined]  # Verify borrowed sink ownership.


def test_queue_concurrent_close_drains_before_all_callers_return() -> None:
    """
    Verify queue concurrent close drains before all callers return.
    """
    sink: ControlledSink = ControlledSink()
    handler: QueueHandler = QueueHandler(sink, capacity=1)
    LogManager(handlers=[handler]).get_logger().info("last")
    assert sink.entered.wait(2)
    start: threading.Barrier = threading.Barrier(3)

    def close() -> int:
        """
        Finish owned work and release this resource.
        """
        start.wait(2)
        handler.close()
        return len(sink.records)

    pool: ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures: list[Future[int]] = [pool.submit(close) for _ in range(2)]
        try:
            start.wait(2)
        finally:
            sink.allow_emit.set()
        assert [future.result(2) for future in futures] == [1, 1]


@pytest.mark.parametrize("options", [{"capacity": 0}, {"overflow": "invalid"}])
def test_queue_configuration_validation(
    options: dict[str, Any],
) -> None:
    """
    Verify queue configuration validation.

    Args:
        options: Configuration values exercised by the parametrized test.
    """
    with pytest.raises(ValueError):
        QueueHandler(logging.NullHandler(), **options)
