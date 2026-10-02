"""
Bounded delivery with interruptible producer waits and drainable worker ownership.
"""

from __future__ import annotations

import copy
import logging
import queue
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Literal

from ._runtime import ProcessGuard, deadline, remaining


@dataclass(frozen=True, slots=True)
class QueueStats:
    """
    Immutable snapshot of accepted, dropped, failed, and queued record counts.
    """

    accepted: int
    dropped: int
    failed: int
    queued: int


@dataclass(slots=True)
class _Barrier:
    """
    Completion marker for records submitted before a flush.
    """

    done: threading.Event = field(default_factory=threading.Event)


class QueueHandler(
    logging.Handler,
):
    """
    One worker owns sink I/O and cleanup. Close stops acceptance before draining.

    Timeout leaves the worker draining; close can be retried. Nested event fields
    are copied before submission by default. Instances cannot cross processes.
    """

    def __init__(
        self: QueueHandler,
        sink: logging.Handler,
        *,
        capacity: int = 1024,
        overflow: Literal["block", "drop", "raise"] = "drop",
        enqueue_timeout: float | None = None,
        copy_fields: bool = True,
        close_sink: bool = False,
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            sink: Handler receiving records on the delivery worker.
            capacity: Maximum number of buffered records or retained keys.
            overflow: Policy applied when the bounded buffer is full.
            enqueue_timeout: Maximum seconds a producer may wait for queue capacity.
            copy_fields: Whether to deep-copy structured fields before enqueueing.
            close_sink: Whether the delivery worker closes its sink during cleanup.
        """
        if not isinstance(capacity, int) or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        if overflow not in {"block", "drop", "raise"}:
            raise ValueError("Unknown overflow policy")
        deadline(enqueue_timeout)
        super().__init__()
        self._guard: ProcessGuard = ProcessGuard()
        self._sink: logging.Handler = sink
        self._capacity: int = capacity
        self._overflow: Literal["block", "drop", "raise"] = overflow
        self._enqueue_timeout: float | None = enqueue_timeout
        self._copy_fields: bool = copy_fields
        self._close_sink: bool = close_sink
        self._condition: threading.Condition = threading.Condition()
        self._items: deque[logging.LogRecord | _Barrier] = deque()
        self._closing: bool
        self._finished: bool
        self._closing = self._finished = False
        self._accepted: int
        self._dropped: int
        self._failed: int
        self._queued: int
        self._accepted = self._dropped = self._failed = self._queued = 0
        self._last_error: BaseException | None = None
        self._fatal: BaseException | None = None
        self._worker: threading.Thread = threading.Thread(
            target=self._run, name="tix-log", daemon=True
        )
        self._worker.start()

    @property
    def stats(
        self: QueueHandler,
    ) -> QueueStats:
        """
        Return a synchronized snapshot of queue delivery counters.
        """
        self._guard.check()
        with self._condition:
            return QueueStats(self._accepted, self._dropped, self._failed, self._queued)

    @property
    def last_error(
        self: QueueHandler,
    ) -> BaseException | None:
        """
        Return the most recently recorded sink failure.
        """
        self._guard.check()
        with self._condition:
            return self._last_error

    def check_access(
        self: QueueHandler,
    ) -> None:
        """
        Reject inherited instances and callbacks that would wait on their own worker.
        """
        self._guard.check()
        if threading.current_thread() is self._worker:
            raise RuntimeError("Queue sink cannot reenter its own queue")

    def handle(
        self: QueueHandler,
        record: logging.LogRecord,
    ) -> bool:
        """
        Apply record filters before submitting the accepted record.

        Args:
            record: Log record to render or deliver.
        """
        self.check_access()
        filtered: bool = self.filter(record)
        if not filtered:
            return False
        self.emit(filtered if isinstance(filtered, logging.LogRecord) else record)
        return True

    def emit(
        self: QueueHandler,
        record: logging.LogRecord,
    ) -> None:
        """
        Deliver the record or event through the configured execution path.

        Args:
            record: Log record to render or deliver.
        """
        self.check_access()
        end: float | None = deadline(self._enqueue_timeout)
        copied: logging.LogRecord = copy.copy(record)
        if record.args:
            copied.msg, copied.args = record.getMessage(), ()
        if hasattr(record, "event"):
            fields: dict[Any, Any] = dict(record.event)
            copied.event = copy.deepcopy(fields) if self._copy_fields else fields
        with self._condition:
            while self._queued >= self._capacity and not self._closing:
                if self._overflow != "block" or remaining(end) == 0:
                    self._dropped += 1
                    if self._overflow != "drop":
                        raise queue.Full
                    return
                self._condition.wait(remaining(end))
            self._raise_fatal()
            if self._closing:
                raise RuntimeError("QueueHandler is closed")
            self._items.append(copied)
            self._accepted += 1
            self._queued += 1
            self._condition.notify_all()

    def _failure(
        self: QueueHandler,
        error: BaseException,
    ) -> None:
        """
        Record a sink failure without losing its cause.

        Args:
            error: Failure associated with this operation.
        """
        with self._condition:
            self._failed += 1
            self._last_error = error

    def _raise_fatal(
        self: QueueHandler,
    ) -> None:
        """
        Raise when the delivery worker has failed permanently.
        """
        if self._fatal is not None:
            raise RuntimeError("Queue worker failed") from self._fatal

    def _run(
        self: QueueHandler,
    ) -> None:
        """
        Drain queued work and finalize the sink on its owning worker.
        """
        try:
            while True:
                with self._condition:
                    self._condition.wait_for(lambda: bool(self._items) or self._closing)
                    if not self._items:
                        break
                    current: logging.LogRecord | _Barrier = self._items.popleft()
                    if isinstance(current, logging.LogRecord):
                        self._queued -= 1
                    self._condition.notify_all()
                error: Exception
                try:
                    if isinstance(current, _Barrier):
                        self._sink.flush()
                    elif current.levelno >= self._sink.level:
                        self._sink.handle(current)
                except Exception as error:
                    self._failure(error)
                finally:
                    if isinstance(current, _Barrier):
                        current.done.set()
        except BaseException as error:
            self._failure(error)
            with self._condition:
                self._fatal = error
        finally:
            try:
                self._sink.flush()
            except BaseException as error:
                self._failure(error)
            try:
                if self._close_sink:
                    self._sink.close()
            except BaseException as error:
                self._failure(error)
            with self._condition:
                self._closing = self._finished = True
                self._dropped += self._queued
                self._queued = 0
                item: logging.LogRecord | _Barrier
                for item in self._items:
                    if isinstance(item, _Barrier):
                        item.done.set()
                self._items.clear()
                self._condition.notify_all()

    def flush(
        self: QueueHandler,
        timeout: float | None = None,
    ) -> None:
        """
        Wait for previously accepted output to reach the sink.

        Args:
            timeout: Maximum wait in seconds, or None for no deadline.
        """
        if not self._guard.owned:
            return  # logging.shutdown may visit inherited handlers after fork.
        self.check_access()
        end: float | None = deadline(timeout)
        barrier: _Barrier = _Barrier()
        with self._condition:
            if self._closing:
                if not self._condition.wait_for(lambda: self._finished, remaining(end)):
                    raise TimeoutError("Queue flush timed out during close")
                self._raise_fatal()
                return
            self._raise_fatal()
            self._items.append(barrier)
            self._condition.notify_all()
        if not barrier.done.wait(remaining(end)):
            with self._condition:
                try:
                    self._items.remove(barrier)
                except ValueError:
                    pass  # Worker already owns this barrier.
            raise TimeoutError("Queue flush timed out")
        with self._condition:
            self._raise_fatal()

    def close(
        self: QueueHandler,
        timeout: float | None = None,
    ) -> None:
        """
        Finish owned work and release this resource.

        Args:
            timeout: Maximum wait in seconds, or None for no deadline.
        """
        if not self._guard.owned:
            return
        self.check_access()
        end: float | None = deadline(timeout)
        with self._condition:
            self._closing = True
            self._condition.notify_all()
            if not self._condition.wait_for(lambda: self._finished, remaining(end)):
                raise TimeoutError("Queue close timed out; worker is still draining")
        self._worker.join(remaining(end))
        if self._worker.is_alive():
            raise TimeoutError("Queue worker has not exited")
        super().close()
        with self._condition:
            self._raise_fatal()
