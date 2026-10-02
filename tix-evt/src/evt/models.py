"""
Public envelopes, delivery results and subscription ownership.
"""

from __future__ import annotations

import weakref
from collections.abc import Callable, Hashable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType, TracebackType
from typing import Any

ANY: object = object()


@dataclass(frozen=True, slots=True)
class Event:
    """
    Immutable event envelope with caller-owned payload and shallow-frozen metadata.
    """

    topic: Hashable
    payload: Any = None
    sender: object = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(
        self: Event,
    ) -> None:
        """
        Freeze a shallow copy of event metadata.
        """
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class Outcome:
    """
    Value or ordinary exception returned by a single subscription.
    """

    subscription_id: int
    value: Any = None
    error: Exception | None = None


@dataclass(frozen=True, slots=True)
class DispatchResult:
    """
    Ordered callback outcomes with optional aggregate error propagation.
    """

    outcomes: tuple[Outcome, ...] = ()

    @property
    def errors(
        self: DispatchResult,
    ) -> tuple[Exception, ...]:
        """
        Return the ordinary failures recorded in this result.
        """
        return tuple(item.error for item in self.outcomes if item.error is not None)

    def raise_errors(
        self: DispatchResult,
    ) -> None:
        """
        Raise an exception group when any recorded operation failed.
        """
        errors: tuple[Exception, ...]
        if errors := self.errors:
            raise ExceptionGroup("Event dispatch failed", list(errors))


class Subscription:
    """
    An idempotent disconnect handle; keeping it does not keep a bus alive.
    """

    def __init__(
        self: Subscription,
        identifier: int,
        cancel: Callable[[int], bool],
    ) -> None:
        """
        Initialize the instance and its owned state.

        Args:
            identifier: Identifier of the subscription or worker.
            cancel: Bound callback removing a subscription by identifier.
        """
        self.id: int = identifier
        self._cancel: weakref.WeakMethod[Callable[[int], bool]] = weakref.WeakMethod(cancel)

    def cancel(
        self: Subscription,
    ) -> bool:
        """
        Request cancellation while preserving the first recorded cause.
        """
        callback: Callable[[int], bool] | None = self._cancel()
        return bool(callback(self.id)) if callback is not None else False

    def __enter__(
        self: Subscription,
    ) -> Subscription:
        """
        Enter the managed scope and return this instance.
        """
        return self

    def __exit__(
        self: Subscription,
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
        self.cancel()
