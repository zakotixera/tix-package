"""
Ordered, instance-local event dispatch.
"""

from __future__ import annotations

from .bus import EventBus
from .models import ANY, DispatchResult, Event, Outcome, Subscription
from .protocols import EventSource
from .stream import EventStream

__all__: list[str] = [
    "ANY",
    "DispatchResult",
    "Event",
    "EventBus",
    "EventSource",
    "EventStream",
    "Outcome",
    "Subscription",
]
