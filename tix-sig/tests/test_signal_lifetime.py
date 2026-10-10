"""
Signal adapter ownership and reclamation across installation scopes.
"""

from __future__ import annotations

import gc
import signal
import weakref

from sig import ShutdownManager, SignalAdapter


def test_closed_adapters_are_not_retained_or_polled() -> None:
    """
    Reclaim discarded adapters while the manager remains alive and usable.
    """
    manager: ShutdownManager = ShutdownManager()
    references: list[weakref.ReferenceType[SignalAdapter]] = []
    _index: int
    adapter: SignalAdapter
    for _index in range(20):
        with manager.signals([signal.SIGINT]) as adapter:
            references.append(weakref.ref(adapter))
    del adapter
    gc.collect()
    reference: weakref.ReferenceType[SignalAdapter]
    for reference in references:
        assert reference() is None
    assert not manager._adapters
    assert manager.poll() == 0
    manager.shutdown().raise_errors()


def test_installed_adapter_survives_without_a_caller_reference() -> None:
    """
    Let OS handler ownership keep an installed adapter available to manager polling.
    """
    previous: object = signal.getsignal(signal.SIGINT)
    manager: ShutdownManager = ShutdownManager()
    adapter: SignalAdapter = manager.signals([signal.SIGINT]).install()
    reference: weakref.ReferenceType[SignalAdapter] = weakref.ref(adapter)
    del adapter
    gc.collect()
    retained: SignalAdapter | None = reference()
    assert retained is not None
    try:
        signal.raise_signal(signal.SIGINT)
        assert manager.poll() == 1
        assert manager.token.reason == "SIGINT"
    finally:
        retained.close()
    assert signal.getsignal(signal.SIGINT) == previous
    del retained
    gc.collect()
    assert reference() is None
    assert not manager._adapters


def test_retained_adapter_can_be_reinstalled() -> None:
    """
    Continue polling an explicitly retained adapter after closing and reinstalling it.
    """
    manager: ShutdownManager = ShutdownManager()
    adapter: SignalAdapter = manager.signals([signal.SIGINT])
    with adapter:
        pass
    with adapter:
        signal.raise_signal(signal.SIGINT)
        assert manager.poll() == 1
    assert manager.token.reason == "SIGINT"


def test_closing_adapter_delivers_pending_signals_before_collection() -> None:
    """
    Preserve pending shutdown and escalation requests when their adapter scope ends.
    """
    manager: ShutdownManager = ShutdownManager()
    with manager.signals([signal.SIGINT]):
        signal.raise_signal(signal.SIGINT)
        signal.raise_signal(signal.SIGINT)
        assert not manager.token.cancelled
    gc.collect()
    assert manager.token.reason == "SIGINT"
    assert manager.escalated
    assert not manager._adapters
