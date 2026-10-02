"""
Cooperative cancellation and explicit shutdown orchestration.
"""

from __future__ import annotations

from .manager import ShutdownManager
from .models import HookResult, Phase, ShutdownContext, ShutdownReport, State
from .signals import SignalAdapter
from .token import CancellationToken, ShutdownRequested

__all__: list[str] = [
    "CancellationToken",
    "HookResult",
    "Phase",
    "ShutdownContext",
    "ShutdownManager",
    "ShutdownReport",
    "ShutdownRequested",
    "SignalAdapter",
    "State",
]
