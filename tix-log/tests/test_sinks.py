"""
Contracts for trace records, text I/O, and handler lifecycle synchronization.
"""

from __future__ import annotations

import io
import json
import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Literal

import pytest
from log import (
    DEBUG,
    TRACE,
    BoundLogger,
    FileSink,
    JsonFormatter,
    LogManager,
    QueueHandler,
    SinkHandler,
    StandardStreamSink,
    StreamSink,
)


class CountingStream(
    io.StringIO,
):
    """
    Track flushes while retaining normal in-memory text stream behavior.
    """

    def __init__(
        self: CountingStream,
    ) -> None:
        """
        Initialize the text buffer and flush counter.
        """
        super().__init__()
        self.flushes: int = 0

    def flush(
        self: CountingStream,
    ) -> None:
        """
        Count explicit flush operations.
        """
        self.flushes += 1
        super().flush()


def test_trace_filtering_names_and_global_isolation() -> None:
    """
    TRACE stays local while JSON, standard formatters, and queues preserve its name.
    """
    names: dict[str, int] = logging.getLevelNamesMapping()
    plain: io.StringIO = io.StringIO()
    structured: io.StringIO = io.StringIO()
    handler: SinkHandler = SinkHandler(
        StreamSink(plain), formatter=logging.Formatter("%(levelname)s:%(message)s")
    )
    queued: QueueHandler = QueueHandler(
        SinkHandler(StreamSink(structured), formatter=JsonFormatter()), close_sink=True
    )
    manager: LogManager
    with LogManager(level=DEBUG, handlers=[handler, queued], close_handlers=True) as manager:
        logger: BoundLogger = manager.get_logger()
        assert TRACE < DEBUG == logging.DEBUG
        assert not logger.trace(lambda: pytest.fail("disabled trace evaluated"))
        manager.set_level(TRACE)
        assert logger.enabled(TRACE)
        assert logger.trace("detail", step=1)
        assert logger.log(TRACE, "generic")
    assert plain.getvalue() == "TRACE:detail\nTRACE:generic\n"
    events: list[dict[str, Any]] = [json.loads(line) for line in structured.getvalue().splitlines()]
    assert [event["level"] for event in events] == ["trace", "trace"]
    assert events[0]["step"] == 1
    assert logging.getLevelNamesMapping() == names
    record: logging.LogRecord = logging.makeLogRecord({"levelno": TRACE, "msg": "external"})
    assert json.loads(JsonFormatter().format(record))["level"] == "trace"


def test_trace_handler_threshold_and_named_exception_arguments() -> None:
    """
    Sink filtering and explicit exception arguments work for convenience methods.
    """
    output: io.StringIO = io.StringIO()
    handler: SinkHandler = SinkHandler(
        StreamSink(output), level=logging.DEBUG, formatter=JsonFormatter()
    )
    manager: LogManager
    with LogManager(level=TRACE, handlers=[handler], close_handlers=True) as manager:
        logger: BoundLogger = manager.get_logger()
        logger.trace("sink excludes this")
        try:
            raise ValueError("failure detail")
        except ValueError:
            logger.debug("debug exception", exc_info=True)
    event: dict[str, Any] = json.loads(output.getvalue())
    assert event["level"] == "debug"
    assert "ValueError: failure detail" in event["exception"]
    assert "exc_info" not in event


def test_flush_counts_severity_manual_flush_and_borrowed_ownership() -> None:
    """
    Count and severity flushes reset the counter without closing borrowed resources.
    """
    stream: CountingStream = CountingStream()
    sink: StreamSink = StreamSink(stream)
    handler: SinkHandler = SinkHandler(sink, flush_every=3)
    record: logging.LogRecord = logging.makeLogRecord({"msg": "line", "levelno": logging.INFO})
    handler.handle(record)
    handler.handle(record)
    assert stream.flushes == 0
    handler.handle(record)
    assert stream.flushes == 1
    record.levelno = logging.ERROR
    handler.handle(record)
    assert stream.flushes == 2
    handler.flush()
    assert stream.flushes == 3
    handler.close()
    handler.close()
    handler.flush()
    assert stream.flushes == 4
    assert not stream.closed
    sink.write("still borrowed")
    with pytest.raises(RuntimeError, match="closed"):
        handler.handle(record)
    sink.close()
    assert not stream.closed
    with pytest.raises(RuntimeError, match="closed"):
        sink.write("rejected")


def test_manual_flush_mode_and_owned_stream() -> None:
    """
    Manual-only mode defers flushing and closes explicitly owned resources.
    """
    stream: CountingStream = CountingStream()
    handler: SinkHandler = SinkHandler(
        StreamSink(stream, close_stream=True), flush_every=0, flush_level=None, close_sink=True
    )
    handler.emit(logging.makeLogRecord({"msg": "failure", "levelno": logging.CRITICAL}))
    assert stream.flushes == 0
    handler.close()
    assert stream.closed
    assert stream.flushes == 1


def test_standard_logger_filters_and_custom_terminator() -> None:
    """
    The adapter works with ordinary logging loggers and their filtering rules.
    """
    stream: io.StringIO = io.StringIO()
    handler: SinkHandler = SinkHandler(StreamSink(stream), terminator="|", level=logging.WARNING)
    handler.addFilter(lambda record: record.getMessage() != "filtered")
    logger: logging.Logger = logging.Logger("standard", logging.DEBUG)
    logger.addHandler(handler)
    logger.info("low")
    logger.warning("filtered")
    logger.error("accepted")
    handler.close()
    assert stream.getvalue() == "accepted|"


def test_flush_failure_still_closes_owned_stream() -> None:
    """
    Owned resources are released even when the final explicit flush fails.
    """

    class FailingStream(
        io.StringIO,
    ):
        """
        Simulate a failing buffered destination during cleanup.
        """

        def flush(
            self: FailingStream,
        ) -> None:
            """
            Report a destination failure.
            """
            raise OSError("flush failed")

    stream: FailingStream = FailingStream()
    handler: SinkHandler = SinkHandler(StreamSink(stream, close_stream=True), close_sink=True)
    with pytest.raises(OSError, match="flush failed"):
        handler.close()
    assert stream.closed
    handler.close()
    handler.flush()


def test_standard_stream_tracks_redirection_without_closing_it() -> None:
    """
    Standard streams are resolved at write time and remain application-owned.
    """
    sink: StandardStreamSink = StandardStreamSink("stdout")
    first: io.StringIO = io.StringIO()
    second: io.StringIO = io.StringIO()
    with redirect_stdout(first):
        sink.write("first")
        sink.flush()
    with redirect_stdout(second):
        sink.write("second")
        sink.close()
    assert first.getvalue() == "first"
    assert second.getvalue() == "second"
    assert not first.closed and not second.closed


def test_file_lazy_creation_encoding_and_close(
    tmp_path: Path,
) -> None:
    """
    Unused sinks do not create files; active files flush and never reopen after close.

    Args:
        tmp_path: Isolated temporary output directory.
    """
    path: Path = tmp_path / "nested" / "events.log"
    unused: FileSink = FileSink(path, create_parents=True)
    unused.flush()
    unused.close()
    assert not path.parent.exists()
    sink: FileSink = FileSink(path, create_parents=True)
    handler: SinkHandler = SinkHandler(sink, flush_every=0, close_sink=True)
    handler.emit(logging.makeLogRecord({"msg": "你好\nnext", "levelno": logging.INFO}))
    handler.flush()
    assert path.read_bytes() == "你好\nnext\n".encode()
    handler.close()
    with pytest.raises(RuntimeError, match="closed"):
        sink.write("late")
    assert path.read_text(encoding="utf-8") == "你好\nnext\n"


@pytest.mark.parametrize("mode,expected", [("a", "oldnew"), ("w", "new")])
def test_file_modes_and_exclusive_creation(
    tmp_path: Path,
    mode: Literal["a", "w"],
    expected: str,
) -> None:
    """
    Opening mode is applied once, with errors propagated for exclusive creation.

    Args:
        tmp_path: Isolated temporary output directory.
        mode: Append or truncate mode under test.
        expected: Expected text after the sink is closed.
    """
    path: Path = tmp_path / "output.log"
    path.write_text("old", encoding="utf-8")
    sink: FileSink = FileSink(path, mode=mode)
    assert path.read_text() == "old"
    sink.write("new")
    sink.close()
    assert path.read_text() == expected
    exclusive: FileSink = FileSink(path, mode="x")
    with pytest.raises(FileExistsError):
        exclusive.write("rejected")
    exclusive.close()


def test_file_failure_propagates_directly_and_is_reported_by_queue(
    tmp_path: Path,
) -> None:
    """
    Missing-directory failures reach direct callers and queued failure statistics.

    Args:
        tmp_path: Isolated temporary output directory.
    """
    handler: SinkHandler = SinkHandler(FileSink(tmp_path / "missing" / "output"), close_sink=True)
    record: logging.LogRecord = logging.makeLogRecord({"msg": "line", "levelno": logging.INFO})
    with pytest.raises(FileNotFoundError):
        handler.handle(record)
    queued: QueueHandler = QueueHandler(handler, close_sink=True)
    queued.handle(record)
    queued.close(timeout=2)
    assert queued.stats.failed == 1
    assert isinstance(queued.last_error, FileNotFoundError)


def test_parallel_file_writes_remain_complete(
    tmp_path: Path,
) -> None:
    """
    Threads sharing a file sink preserve complete records through buffered writes.

    Args:
        tmp_path: Isolated temporary output directory.
    """
    path: Path = tmp_path / "parallel.log"
    sink: FileSink = FileSink(path)

    def write(
        index: int,
    ) -> None:
        """
        Write a distinct complete line from a producer thread.

        Args:
            index: Producer record identifier.
        """
        sink.write(f"{index}:" + "x" * 1000 + "\n")

    pool: ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(200)))
    sink.close()
    lines: list[str] = path.read_text().splitlines()
    assert len(lines) == 200
    assert {int(line.split(":")[0]) for line in lines} == set(range(200))
    assert all(line.split(":")[1] == "x" * 1000 for line in lines)


def test_handler_close_waits_for_accepted_write() -> None:
    """
    Concurrent close waits until the active stream write completes before cleanup.
    """
    entered: threading.Event = threading.Event()
    release: threading.Event = threading.Event()

    class BlockingStream(
        io.StringIO,
    ):
        """
        Hold a write so closure can race with active output deterministically.
        """

        def write(
            self: BlockingStream,
            text: str,
        ) -> int:
            """
            Wait for the test to release this write.

            Args:
                text: Text accepted after the synchronization barrier.
            """
            entered.set()
            assert release.wait(2)
            return super().write(text)

    stream: BlockingStream = BlockingStream()
    handler: SinkHandler = SinkHandler(StreamSink(stream), close_sink=True)
    pool: ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as pool:
        write: Future[None] = pool.submit(handler.emit, logging.makeLogRecord({"msg": "last"}))
        try:
            assert entered.wait(2)
            close: Future[None] = pool.submit(handler.close)
            assert not close.done()
        finally:
            release.set()
        write.result(2)
        close.result(2)
    assert stream.getvalue() == "last\n"


def test_inherited_sink_and_handler_reject_writes_before_locks() -> None:
    """
    Process guards reject writes and make interpreter cleanup harmless in children.
    """
    stream: CountingStream = CountingStream()
    sink: StreamSink = StreamSink(stream)
    handler: SinkHandler = SinkHandler(sink)
    sink_pid: int = sink._guard.pid
    handler_pid: int = handler._guard.pid
    try:
        sink._guard.pid = handler._guard.pid = -1
        with pytest.raises(RuntimeError, match="another process"):
            sink.write("child")
        with pytest.raises(RuntimeError, match="another process"):
            handler.handle(logging.makeLogRecord({"msg": "child"}))
        sink.flush()
        sink.close()
        handler.flush()
        handler.close()
        assert stream.flushes == 0
    finally:
        sink._guard.pid = sink_pid
        handler._guard.pid = handler_pid
        handler.close()
        sink.close()


def test_short_write_is_reported() -> None:
    """
    A stream that accepts only part of a record must not silently lose the rest.
    """

    class PartialStream(
        io.StringIO,
    ):
        """
        Simulate a short text write.
        """

        def write(
            self: PartialStream,
            text: str,
        ) -> int:
            """
            Accept only the first character.

            Args:
                text: Text whose remainder is intentionally rejected.
            """
            return super().write(text[:1])

    sink: StreamSink = StreamSink(PartialStream())
    with pytest.raises(OSError, match="complete write"):
        sink.write("partial")
    sink.close()
