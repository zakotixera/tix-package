"""
Shutdown contracts and immutable result models.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import IntEnum, StrEnum

from .token import CancellationToken


class Phase(
    IntEnum,
):
    """
    Conventional cleanup phases, ordered from stopping work to releasing resources.
    """

    STOP = 0
    DRAIN = 100
    CLOSE = 200


class State(
    StrEnum,
):
    """
    Lifecycle states of a shutdown manager.
    """

    ACTIVE = "active"
    REQUESTED = "requested"
    RUNNING = "running"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class ShutdownContext:
    """
    Cancellation cause and deadline supplied to a cleanup hook.
    """

    token: CancellationToken
    reason: str
    deadline: float | None

    @property
    def remaining(
        self: ShutdownContext,
    ) -> float | None:
        """
        Return the nonnegative seconds remaining before the deadline.
        """
        return None if self.deadline is None else max(0.0, self.deadline - time.perf_counter())


@dataclass(frozen=True, slots=True)
class HookResult:
    """
    Elapsed time, failure, and skip status for one cleanup hook.
    """

    name: str
    elapsed: float
    error: Exception | None = None
    skipped: bool = False


@dataclass(frozen=True, slots=True)
class ShutdownReport:
    """
    Cached cleanup results and final escalation/interruption status.
    """

    reason: str
    hooks: tuple[HookResult, ...]
    elapsed: float
    escalated: bool
    interrupted: bool = False

    @property
    def errors(
        self: ShutdownReport,
    ) -> tuple[Exception, ...]:
        """
        Return the ordinary failures recorded in this result.
        """
        return tuple(hook.error for hook in self.hooks if hook.error is not None)

    def raise_errors(
        self: ShutdownReport,
    ) -> None:
        """
        Raise an exception group when any recorded operation failed.
        """
        errors: tuple[Exception, ...]
        if errors := self.errors:
            raise ExceptionGroup("Shutdown hooks failed", list(errors))
