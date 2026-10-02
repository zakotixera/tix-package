"""
Instance-local logging configuration and producer-side enrichment.
"""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from types import MappingProxyType, TracebackType
from typing import Any, TypeAlias

from ._delivery import Delivery
from .levels import CRITICAL, DEBUG, ERROR, INFO, TRACE, WARNING

Processor: TypeAlias = Callable[[dict[str, Any]], dict[str, Any] | None]
Message: TypeAlias = str | Callable[[], str]
_RESERVED: frozenset[str] = frozenset({"message", "level", "logger", "timestamp", "exception"})


def _fields(
    fields: Mapping[str, Any],
) -> None:
    """
    Reject fields that collide with the reserved event envelope.

    Args:
        fields: Structured values to bind, validate, or attach to the event.
    """
    overlap: frozenset[str]
    if overlap := _RESERVED.intersection(fields):
        raise ValueError(f"Reserved event fields: {sorted(overlap)}")


class LogManager:
    """
    Own an isolated logger and immutable pipeline; handlers are borrowed by default.

    Configure before concurrent use. ``close`` waits for accepted producer calls.
    Handler I/O and processor exceptions propagate to the calling producer.
    """

    def __init__(
        self: LogManager,
        name: str = "tix",
        *,
        level: int = INFO,
        handlers: Iterable[logging.Handler] = (),
        processors: Iterable[Processor] = (),
        close_handlers: bool = False,
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            name: Name identifying this instance or registered resource.
            level: Minimum severity or severity of the event being evaluated.
            handlers: Standard logging handlers that receive accepted records.
            processors: Ordered transformations applied before record delivery.
            close_handlers: Whether manager cleanup also closes its handlers.
        """
        self._logger: logging.Logger = logging.Logger(name, level)
        self._logger.propagate = False
        self._delivery: Delivery = Delivery(tuple(handlers), level, close_handlers)
        self._processors: tuple[Processor, ...] = tuple(processors)
        self._context: ContextVar[Mapping[str, Any]] = ContextVar(
            f"{name}.context", default=MappingProxyType({})
        )

    @property
    def name(
        self: LogManager,
    ) -> str:
        """
        Return the name of this isolated logger.
        """
        return self._logger.name

    def set_level(
        self: LogManager,
        level: int,
    ) -> None:
        """
        Change early filtering, including for already-created bound loggers.

        Args:
            level: Minimum severity or severity of the event being evaluated.
        """
        self._delivery.set_level(level)

    def enabled(
        self: LogManager,
        level: int,
    ) -> bool:
        """
        Return whether this severity is currently accepted.

        Args:
            level: Minimum severity or severity of the event being evaluated.
        """
        return self._delivery.enabled(level)

    def get_logger(
        self: LogManager,
        **fields: Any,
    ) -> BoundLogger:
        """
        Create an immutable binding facade sharing this manager.

        Args:
            fields: Structured values to bind, validate, or attach to the event.
        """
        return BoundLogger(self, fields)

    @contextmanager
    def context(
        self: LogManager,
        **fields: Any,
    ) -> Iterator[None]:
        """
        Temporarily bind fields in the current context; nested scopes restore exactly.

        Args:
            fields: Structured values to bind, validate, or attach to the event.
        """
        _fields(fields)
        token: Token[Mapping[str, Any]] = self._context.set({**self._context.get(), **fields})
        try:
            yield
        finally:
            self._context.reset(token)

    def _emit(
        self: LogManager,
        level: int,
        message: Message,
        bound: Mapping[str, Any],
        fields: dict[str, Any],
        exc_info: bool,
    ) -> bool:
        """
        Lease delivery while enriching and publishing an accepted event.

        Args:
            level: Minimum severity or severity of the event being evaluated.
            message: Message text or a callable evaluated only when logging is enabled.
            bound: Previously bound fields merged into the event.
            fields: Structured values to bind, validate, or attach to the event.
            exc_info: Whether to attach the currently handled exception.
        """
        if not self._delivery.begin(level):
            return False
        try:
            return self._build_emit(level, message, bound, fields, exc_info)
        finally:
            self._delivery.end()

    def _build_emit(
        self: LogManager,
        level: int,
        message: Message,
        bound: Mapping[str, Any],
        fields: dict[str, Any],
        exc_info: bool,
    ) -> bool:
        """
        Build the structured record and pass it to configured sinks.

        Args:
            level: Minimum severity or severity of the event being evaluated.
            message: Message text or a callable evaluated only when logging is enabled.
            bound: Previously bound fields merged into the event.
            fields: Structured values to bind, validate, or attach to the event.
            exc_info: Whether to attach the currently handled exception.
        """
        _fields(fields)
        event: dict[str, Any] = {
            **bound,
            **self._context.get(),
            **fields,
            "message": message() if callable(message) else message,
            "level": "trace" if level == TRACE else logging.getLevelName(level).lower(),
            "logger": self.name,
            "timestamp": time.time(),
        }
        processor: Callable[[dict[str, Any]], dict[str, Any] | None]
        for processor in self._processors:
            processed: dict[str, Any] | None = processor(event)
            if processed is None:
                return False
            event = processed
        record: logging.LogRecord = self._logger.makeRecord(
            self.name,
            level,
            "",
            0,
            event["message"],
            (),
            sys.exc_info() if exc_info else None,
            extra={"event": event},
        )
        if level == TRACE:
            record.levelname = "TRACE"
        # Calling handlers directly avoids logging.lastResort and global disable state.
        handler: logging.Handler
        for handler in self._delivery.handlers:
            if level >= handler.level:
                handler.handle(record)
        return True

    def flush(
        self: LogManager,
    ) -> None:
        """
        Wait for previously accepted output to reach the sink.
        """
        self._delivery.flush()

    def close(
        self: LogManager,
    ) -> None:
        """
        Finish owned work and release this resource.
        """
        self._delivery.close()

    def __enter__(
        self: LogManager,
    ) -> LogManager:
        """
        Enter the managed scope and return this instance.
        """
        with self._delivery.lease():
            pass
        return self

    def __exit__(
        self: LogManager,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """
        Release the resources owned by this managed scope.

        Args:
            exc_type: Exception type leaving the managed scope, if any.
            exc: Exception leaving the managed scope, if any.
            tb: Traceback associated with the exception, if any.
        """
        self.close()


class BoundLogger:
    """
    A cheap immutable binding facade; the manager controls delivery.
    """

    __slots__: tuple[str, ...] = ("_manager", "_fields")

    def __init__(
        self: BoundLogger,
        manager: LogManager,
        fields: Mapping[str, Any],
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            manager: Manager whose configuration and delivery this object shares.
            fields: Structured values to bind, validate, or attach to the event.
        """
        _fields(fields)
        self._manager: LogManager = manager
        self._fields: MappingProxyType[str, Any] = MappingProxyType(dict(fields))

    def bind(
        self: BoundLogger,
        **fields: Any,
    ) -> BoundLogger:
        """
        Return a new logger with the supplied fields merged into its bindings.

        Args:
            fields: Structured values to bind, validate, or attach to the event.
        """
        return BoundLogger(self._manager, {**self._fields, **fields})

    def unbind(
        self: BoundLogger,
        *keys: str,
    ) -> BoundLogger:
        """
        Return a new logger without the requested field bindings.

        Args:
            keys: Field names to remove or redact.
        """
        return BoundLogger(self._manager, {k: v for k, v in self._fields.items() if k not in keys})

    def enabled(
        self: BoundLogger,
        level: int,
    ) -> bool:
        """
        Return whether this severity is currently accepted.

        Args:
            level: Minimum severity or severity of the event being evaluated.
        """
        return self._manager.enabled(level)

    def log(
        self: BoundLogger,
        level: int,
        message: Message,
        *,
        exc_info: bool = False,
        **fields: Any,
    ) -> bool:
        """
        Publish a structured event at the requested severity.

        Args:
            level: Minimum severity or severity of the event being evaluated.
            message: Message text or a callable evaluated only when logging is enabled.
            exc_info: Whether to attach the currently handled exception.
            fields: Structured values to bind, validate, or attach to the event.
        """
        return self._manager._emit(level, message, self._fields, fields, exc_info)

    def trace(
        self: BoundLogger,
        message: Message,
        *,
        exc_info: bool = False,
        **fields: Any,
    ) -> bool:
        """
        Publish a trace event below DEBUG when that severity is enabled.

        Args:
            message: Text or a callable evaluated only when logging is enabled.
            exc_info: Whether to attach the currently handled exception.
            fields: Structured values attached to the event.
        """
        return self._manager._emit(TRACE, message, self._fields, fields, exc_info)

    def debug(
        self: BoundLogger,
        message: Message,
        *,
        exc_info: bool = False,
        **fields: Any,
    ) -> bool:
        """
        Publish a debug event when that severity is enabled.

        Args:
            message: Message text or a callable evaluated only when logging is enabled.
            exc_info: Whether to attach the currently handled exception.
            fields: Structured values to bind, validate, or attach to the event.
        """
        return self._manager._emit(DEBUG, message, self._fields, fields, exc_info)

    def info(
        self: BoundLogger,
        message: Message,
        *,
        exc_info: bool = False,
        **fields: Any,
    ) -> bool:
        """
        Publish an informational event.

        Args:
            message: Message text or a callable evaluated only when logging is enabled.
            exc_info: Whether to attach the currently handled exception.
            fields: Structured values to bind, validate, or attach to the event.
        """
        return self._manager._emit(INFO, message, self._fields, fields, exc_info)

    def warning(
        self: BoundLogger,
        message: Message,
        *,
        exc_info: bool = False,
        **fields: Any,
    ) -> bool:
        """
        Publish a warning event.

        Args:
            message: Message text or a callable evaluated only when logging is enabled.
            exc_info: Whether to attach the currently handled exception.
            fields: Structured values to bind, validate, or attach to the event.
        """
        return self._manager._emit(WARNING, message, self._fields, fields, exc_info)

    def error(
        self: BoundLogger,
        message: Message,
        *,
        exc_info: bool = False,
        **fields: Any,
    ) -> bool:
        """
        Publish an error event.

        Args:
            message: Message text or a callable evaluated only when logging is enabled.
            exc_info: Whether to attach the currently handled exception.
            fields: Structured values to bind, validate, or attach to the event.
        """
        return self._manager._emit(ERROR, message, self._fields, fields, exc_info)

    def critical(
        self: BoundLogger,
        message: Message,
        *,
        exc_info: bool = False,
        **fields: Any,
    ) -> bool:
        """
        Publish a critical event.

        Args:
            message: Message text or a callable evaluated only when logging is enabled.
            exc_info: Whether to attach the currently handled exception.
            fields: Structured values to bind, validate, or attach to the event.
        """
        return self._manager._emit(CRITICAL, message, self._fields, fields, exc_info)

    def exception(
        self: BoundLogger,
        message: Message,
        **fields: Any,
    ) -> bool:
        """
        Publish an error event with the current exception attached.

        Args:
            message: Message text or a callable evaluated only when logging is enabled.
            fields: Structured values to bind, validate, or attach to the event.
        """
        return self._manager._emit(ERROR, message, self._fields, fields, True)
