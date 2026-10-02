"""
Process ownership and monotonic deadlines, kept local to this distribution.
"""

from __future__ import annotations

import math
import os
import time
from typing import NoReturn


class ProcessGuard:
    """
    Reject runtime state access outside its creating process.
    """

    def __init__(
        self: ProcessGuard,
    ) -> None:
        """
        Initialize the instance and its owned state.
        """
        self.pid: int = os.getpid()

    @property
    def owned(
        self: ProcessGuard,
    ) -> bool:
        """
        Return whether the current process created this resource.
        """
        return os.getpid() == self.pid

    def check(
        self: ProcessGuard,
    ) -> None:
        """
        Reject access from a process that inherited this resource.
        """
        if not self.owned:
            raise RuntimeError(
                "Object belongs to another process; create a new instance in the child"
            )

    def __getstate__(
        self: ProcessGuard,
    ) -> NoReturn:
        """
        Reject serialization of process-local runtime state.
        """
        raise TypeError("Live managers are process-local; pass configuration to child processes")


def deadline(
    timeout: float | None,
) -> float | None:
    """
    Validate a duration and convert it to an absolute monotonic deadline.

    Args:
        timeout: Maximum wait in seconds, or None for no deadline.
    """
    if timeout is None:
        return None
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout must be finite and nonnegative")
    return time.perf_counter() + timeout


def remaining(
    end: float | None,
) -> float | None:
    """
    Return the nonnegative seconds remaining before the deadline.

    Args:
        end: Absolute perf-counter deadline, or None for no deadline.
    """
    return None if end is None else max(0.0, end - time.perf_counter())
