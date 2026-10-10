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
        self._active: set[int] = set()
        self._sequence: int = 0
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
    ) -> Iterator[int]:
        """
        Hold an ordered lease for the duration of a lifecycle operation.
        """
        identifier: int | None = self.begin()
        assert identifier is not None
        try:
            yield identifier
        finally:
            self.end(identifier)

    def begin(
        self: Delivery,
        level: int | None = None,
    ) -> int | None:
        """
        Return an ordered lease ID, or None when the level is disabled.

        Args:
            level: Minimum severity or severity of the event being evaluated.
        """
        self._guard.check()
        with self._condition:
            if self._closing:
                raise RuntimeError("LogManager is closed")
            if level is not None and level < self._level:
                return None
            self._sequence += 1
            self._active.add(self._sequence)
            depth: int = getattr(self._local, "depth", 0) + 1
            self._local.depth = depth
            return self._sequence

    def end(
        self: Delivery,
        identifier: int,
    ) -> None:
        """
        Release a lease and wake flushers and closers.

        Args:
            identifier: Lease ID returned by begin on this thread.
        """
        with self._condition:
            self._local.depth -= 1
            self._active.remove(identifier)
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
        identifier: int
        with self.lease() as identifier:
            with self._condition:
                # Flush starts when its lease is the oldest still active. Later
                # producers, including recursive logging, remain free to run.
                self._condition.wait_for(lambda: min(self._active) == identifier)
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
                self._condition.wait_for(lambda: not self._active)
            except BaseException:
                self._closing = False
                self._closer = None
                self._condition.notify_all()
                raise
        errors: list[BaseException] = []
        try:
            handler: logging.Handler
            for handler in self.handlers:
                error: BaseException
                try:
                    handler.flush()
                except BaseException as error:
                    errors.append(error)
                if self._owned:
                    try:
                        handler.close()
                    except BaseException as error:
                        errors.append(error)
        finally:
            with self._condition:
                self._closed = True
                self._closer = None
                self._condition.notify_all()
        if errors:
            raise BaseExceptionGroup("Log handler cleanup failed", errors)
