"""
Lease-based sink lifecycle; callbacks never run under the lifecycle lock.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from ._runtime import ProcessGuard
from .queue import QueueHandler


class Delivery:
    """
    Coordinate producer leases and handler cleanup without holding locks across callbacks.
    """

    def __init__(
        self: Delivery,
        handlers: tuple[logging.Handler, ...],
        level: int,
        owned: bool,
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            handlers: Standard logging handlers that receive accepted records.
            level: Minimum severity or severity of the event being evaluated.
            owned: Whether this component owns handler closure.
        """
        self._guard: ProcessGuard = ProcessGuard()
        self.handlers: tuple[logging.Handler, ...] = handlers
        self._level: int = level
        self._owned: bool = owned
        self._condition: threading.Condition = threading.Condition()
        self._local: threading.local = threading.local()
        self._active: int = 0
        self._closing: bool
        self._closed: bool
        self._closing = self._closed = False
        self._closer: int | None = None

    def set_level(
        self: Delivery,
        level: int,
    ) -> None:
        """
        Change the severity threshold while delivery remains open.

        Args:
            level: Minimum severity or severity of the event being evaluated.
        """
        self._guard.check()
        if not isinstance(level, int):
            raise TypeError("level must be an integer logging level")
        with self._condition:
            if self._closing:
                raise RuntimeError("LogManager is closed")
            self._level = level

    def enabled(
        self: Delivery,
        level: int,
    ) -> bool:
        """
        Return whether this severity is currently accepted.

        Args:
            level: Minimum severity or severity of the event being evaluated.
        """
        self._guard.check()
        with self._condition:
            return not self._closing and level >= self._level

    @contextmanager
    def lease(
        self: Delivery,
        level: int | None = None,
    ) -> Iterator[bool]:
        """
        Hold a producer lease for the duration of a managed operation.

        Args:
            level: Minimum severity or severity of the event being evaluated.
        """
        enabled: bool = self.begin(level)
        try:
            yield enabled
        finally:
            if enabled:
                self.end()

    def begin(
        self: Delivery,
        level: int | None = None,
    ) -> bool:
        """
        Acquire a producer lease when the requested level is enabled.

        Args:
            level: Minimum severity or severity of the event being evaluated.
        """
        self._guard.check()
        with self._condition:
            if self._closing:
                raise RuntimeError("LogManager is closed")
            enabled: bool = level is None or level >= self._level
            if enabled:
                self._active += 1
                depth: int = getattr(self._local, "depth", 0) + 1
                self._local.depth = depth
        return enabled

    def end(
        self: Delivery,
    ) -> None:
        """
        Release the producer lease and wake waiting closers.
        """
        with self._condition:
            self._local.depth -= 1
            self._active -= 1
            if self._closing and not self._active:
                self._condition.notify_all()

    def _check_lifecycle(
        self: Delivery,
    ) -> None:
        """
        Reject lifecycle calls that would wait for their own callback.
        """
        self._guard.check()
        if getattr(self._local, "depth", 0) or self._closer == threading.get_ident():
            raise RuntimeError("Cannot flush or close a manager from its own callback")
        handler: logging.Handler
        for handler in self.handlers:
            if isinstance(handler, QueueHandler):
                handler.check_access()

    def flush(
        self: Delivery,
    ) -> None:
        """
        Wait for previously accepted output to reach the sink.
        """
        self._check_lifecycle()
        with self.lease():
            handler: logging.Handler
            for handler in self.handlers:
                handler.flush()

    def close(
        self: Delivery,
    ) -> None:
        """
        Finish owned work and release this resource.
        """
        self._check_lifecycle()
        with self._condition:
            while self._closing and not self._closed:
                self._condition.wait()
            if self._closed:
                return
            self._closing = True
            self._closer = threading.get_ident()
            try:
                self._condition.wait_for(lambda: self._active == 0)
            except BaseException:
                self._closing = False
                self._closer = None
                self._condition.notify_all()
                raise
        errors: list[Exception] = []
        try:
            handler: logging.Handler
            for handler in self.handlers:
                error: Exception
                try:
                    handler.flush()
                except Exception as error:
                    errors.append(error)
                if self._owned:
                    try:
                        handler.close()
                    except Exception as error:
                        errors.append(error)
        finally:
            with self._condition:
                self._closed = True
                self._closer = None
                self._condition.notify_all()
        if errors:
            raise ExceptionGroup("Log handler cleanup failed", errors)
