"""
Standard logging integration for independently reusable text sinks.
"""

from __future__ import annotations

import logging

from ._runtime import ProcessGuard
from .levels import ERROR, NOTSET
from .sinks import TextSink


class SinkHandler(
    logging.Handler,
):
    """
    Format records and control flushing; I/O failures propagate to the caller.
    """

    def __init__(
        self: SinkHandler,
        sink: TextSink,
        *,
        level: int = NOTSET,
        formatter: logging.Formatter | None = None,
        terminator: str = "\n",
        flush_every: int = 1,
        flush_level: int | None = ERROR,
        close_sink: bool = False,
    ) -> None:
        """
        Configure record rendering and sink ownership before concurrent use.

        Args:
            sink: Destination receiving rendered text.
            level: Minimum record severity accepted by a logger or queue.
            formatter: Standard logging formatter; defaults to message text.
            terminator: Text appended to every rendered record.
            flush_every: Flush after this many records; zero disables count flushing.
            flush_level: Flush immediately at this severity; None disables it.
            close_sink: Whether handler closure also closes the sink.
        """
        if not isinstance(flush_every, int) or flush_every < 0:
            raise ValueError("flush_every must be a nonnegative integer")
        if flush_level is not None and not isinstance(flush_level, int):
            raise TypeError("flush_level must be an integer or None")
        super().__init__(level)
        self._guard: ProcessGuard = ProcessGuard()
        self._sink: TextSink = sink
        self._terminator: str = terminator
        self._flush_every: int = flush_every
        self._flush_level: int | None = flush_level
        self._close_sink: bool = close_sink
        self._pending: int = 0
        self._finished: bool = False
        self.setFormatter(formatter)

    def handle(
        self: SinkHandler,
        record: logging.LogRecord,
    ) -> bool:
        """
        Check process ownership before filters or logging locks are touched.

        Args:
            record: Record passed through standard logging filters and formatting.
        """
        self._guard.check()
        filtered: bool | logging.LogRecord = self.filter(record)
        if not filtered:
            return False
        self.emit(filtered if isinstance(filtered, logging.LogRecord) else record)
        return True

    def emit(
        self: SinkHandler,
        record: logging.LogRecord,
    ) -> None:
        """
        Serialize formatting, writing, and flush decisions, including direct calls.

        Args:
            record: Record to render and write to the configured sink.
        """
        self._guard.check()
        self.acquire()
        try:
            if self._finished:
                raise RuntimeError("SinkHandler is closed")
            self._sink.write(self.format(record) + self._terminator)
            self._pending += 1
            if (self._flush_every and self._pending >= self._flush_every) or (
                self._flush_level is not None and record.levelno >= self._flush_level
            ):
                self.flush()
        finally:
            self.release()

    def flush(
        self: SinkHandler,
    ) -> None:
        """
        Flush accepted writes; inherited and closed handlers are ignored at exit.
        """
        if not self._guard.owned:
            return
        self.acquire()
        try:
            if not self._finished:
                self._sink.flush()
                self._pending = 0
        finally:
            self.release()

    def close(
        self: SinkHandler,
    ) -> None:
        """
        Flush borrowed sinks or close owned sinks exactly once under the I/O lock.
        """
        if not self._guard.owned:
            return
        self.acquire()
        try:
            if self._finished:
                return
            self._finished = True
            try:
                if self._close_sink:
                    self._sink.close()
                else:
                    self._sink.flush()
            finally:
                super().close()
        finally:
            self.release()
