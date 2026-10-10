"""
Exception preservation across standard-library queue preparation and serialization.
"""

from __future__ import annotations

import io
import json
import logging
import logging.handlers
import pickle
import queue
from types import TracebackType
from typing import Any

import pytest
from log import JsonFormatter, LogManager, QueueHandler, SinkHandler, StreamSink


@pytest.mark.parametrize("queued", [False, True])
def test_custom_exception_redaction_overrides_captured_text(
    queued: bool,
) -> None:
    """
    Honor custom exception rendering whenever the original exception is available.

    Args:
        queued: Whether to deliver through the Tix background queue.
    """

    class RedactingFormatter(
        JsonFormatter,
    ):
        """
        Remove sensitive exception text using the standard formatter extension point.
        """

        def formatException(
            self: RedactingFormatter,
            ei: tuple[type[BaseException], BaseException, TracebackType | None]
            | tuple[None, None, None],
        ) -> str:
            """
            Render a traceback with sensitive values replaced.

            Args:
                ei: Exception information provided by the log record.
            """
            return super().formatException(ei).replace("private-value", "[REDACTED]")

    output: io.StringIO = io.StringIO()
    sink: SinkHandler = SinkHandler(StreamSink(output), formatter=RedactingFormatter())
    handler: logging.Handler = QueueHandler(sink, close_sink=True) if queued else sink
    manager: LogManager
    with LogManager(handlers=[handler], close_handlers=True) as manager:
        try:
            raise ValueError("private-value")
        except ValueError:
            manager.get_logger().exception("failed")
    event: dict[str, Any] = json.loads(output.getvalue())
    assert event["message"] == "failed"
    assert "ValueError: [REDACTED]" in event["exception"]
    assert "private-value" not in output.getvalue()


def test_structured_exception_survives_standard_queue_and_pickle() -> None:
    """
    Preserve chained tracebacks in both JSON and ordinary downstream formatters.
    """
    records: queue.Queue[logging.LogRecord] = queue.Queue()
    handler: logging.handlers.QueueHandler = logging.handlers.QueueHandler(records)
    manager: LogManager
    with LogManager(handlers=[handler], close_handlers=True) as manager:
        cause: ValueError
        try:
            try:
                raise ValueError("root cause")
            except ValueError as cause:
                raise RuntimeError("job failed") from cause
        except RuntimeError:
            manager.get_logger(job=42).exception("processing failed")
    record: logging.LogRecord = pickle.loads(pickle.dumps(records.get_nowait()))
    assert record.exc_info is None
    output: io.StringIO = io.StringIO()
    sink: logging.StreamHandler[io.StringIO] = logging.StreamHandler(output)
    sink.setFormatter(JsonFormatter())
    sink.handle(record)
    sink.close()
    event: dict[str, Any] = json.loads(output.getvalue())
    assert event["message"] == "processing failed"
    assert event["job"] == 42
    assert "ValueError: root cause" in event["exception"]
    assert "RuntimeError: job failed" in event["exception"]
    assert "direct cause" in event["exception"]
    assert "RuntimeError: job failed" in logging.Formatter().format(record)


def test_direct_json_uses_the_captured_exception() -> None:
    """
    Capture structured exception text before any downstream handler mutates the record.
    """
    records: list[logging.LogRecord] = []

    class Handler(
        logging.Handler,
    ):
        """
        Collect original records without formatting them.
        """

        def emit(
            self: Handler,
            record: logging.LogRecord,
        ) -> None:
            """
            Retain the delivered record for downstream formatting.

            Args:
                record: Original record carrying the structured exception snapshot.
            """
            records.append(record)

    manager: LogManager
    with LogManager(handlers=[Handler()], close_handlers=True) as manager:
        try:
            raise ValueError("captured")
        except ValueError:
            manager.get_logger().exception("failed")
        manager.get_logger().info("healthy")
    record: logging.LogRecord = records[0]
    original: dict[str, Any] = json.loads(JsonFormatter().format(record))
    record.exc_info = None
    assert json.loads(JsonFormatter().format(record)) == original
    assert "ValueError: captured" in original["exception"]
    assert "exception" not in json.loads(JsonFormatter().format(records[1]))
