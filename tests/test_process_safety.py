from __future__ import annotations

import json
import logging
import os
import pathlib
import subprocess
import sys
import textwrap
from collections.abc import Callable
from typing import Any

import pytest
from evt import EventBus
from log import LogManager, QueueHandler
from sig import CancellationToken, ShutdownManager, SignalAdapter


def test_spawned_processes_construct_independent_managers_and_fan_in_logs() -> None:
    """
    Verify spawned processes construct independent managers and fan in logs.
    """
    root: pathlib.Path = pathlib.Path(__file__).resolve().parents[1]
    result: subprocess.CompletedProcess[str] = subprocess.run(
        [sys.executable, str(root / "examples/process_logging.py")],
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    events: list[Any] = [json.loads(line) for line in result.stdout.splitlines()]
    assert sorted(event["job"] for event in events) == [0, 1, 2]
    assert len({event["process_id"] for event in events}) == 3


@pytest.mark.parametrize(
    "factory,operation",
    [
        (EventBus, lambda obj: obj.emit("t")),
        (LogManager, lambda obj: obj.get_logger().info("t")),
        (CancellationToken, lambda obj: obj.cancel()),
        (ShutdownManager, lambda obj: obj.request()),
        (lambda: SignalAdapter(lambda reason: None), lambda obj: obj.poll()),
    ],
)
def test_process_checks_precede_state_access(
    factory: Callable[[], Any],
    operation: Callable[[Any], object],
) -> None:
    """
    Verify process checks precede state access.

    Args:
        factory: Factory constructing the process-local object under test.
        operation: Operation expected to reject inherited state.
    """
    instance: Any = factory()
    guard: Any = instance._delivery._guard if isinstance(instance, LogManager) else instance._guard
    original: int = guard.pid
    try:
        guard.pid = -1
        with pytest.raises(RuntimeError, match="another process"):
            operation(instance)
    finally:
        guard.pid = original


def test_inherited_queue_cleanup_does_not_touch_parent_worker() -> None:
    """
    Verify inherited queue cleanup does not touch parent worker.
    """
    queued: QueueHandler = QueueHandler(logging.NullHandler())
    original: int = queued._guard.pid
    try:
        queued._guard.pid = -1
        with pytest.raises(RuntimeError, match="another process"):
            queued.handle(logging.makeLogRecord({"msg": "child"}))
        queued.flush()
        queued.close()
        assert queued._worker.is_alive()
    finally:
        queued._guard.pid = original
        queued.close(timeout=1)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX fork only")
def test_fork_rejects_inherited_locked_managers_and_restores_signals() -> None:
    """
    Verify fork rejects inherited locked managers and restores signals.
    """
    code: str = textwrap.dedent("""
        import logging, os, signal, threading, warnings
        from evt import EventBus
        from log import LogManager, QueueHandler, SinkHandler, StreamSink
        import io
        from sig import CancellationToken, ShutdownManager
        bus, logs = EventBus(), LogManager()
        token, shutdown = CancellationToken(), ShutdownManager()
        queued = QueueHandler(logging.NullHandler())
        sink = StreamSink(io.StringIO())
        handler = SinkHandler(sink)
        previous = signal.getsignal(signal.SIGINT)
        adapter = shutdown.signals([signal.SIGINT]).install()
        ready, release = threading.Event(), threading.Event()
        locks = [bus._registry._lock, logs._delivery._condition, token._lock,
                 shutdown._lock, queued._condition, sink._lock, handler.lock]
        def hold():
            for lock in locks:
                lock.acquire()
            ready.set()
            release.wait(10)
            for lock in reversed(locks):
                lock.release()
        thread = threading.Thread(target=hold)
        thread.start()
        assert ready.wait(2)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            pid = os.fork()
        if pid == 0:
            try:
                signal.alarm(3)
                assert signal.getsignal(signal.SIGINT) == previous
                for call in [lambda: bus.emit('t'), lambda: logs.get_logger().info('t'),
                             token.cancel, shutdown.request, adapter.poll,
                             lambda: queued.handle(logging.makeLogRecord({'msg': 'child'})),
                             lambda: sink.write('child'),
                             lambda: handler.handle(logging.makeLogRecord({'msg': 'child'}))]:
                    try:
                        call()
                    except RuntimeError as error:
                        assert 'another process' in str(error)
                    else:
                        raise AssertionError('Inherited manager accepted use')
                queued.close()
                queued.flush()
                sink.flush()
                sink.close()
                handler.flush()
                handler.close()
                with ShutdownManager().signals([signal.SIGINT]):
                    pass
                EventBus().emit('fresh')
                os._exit(0)
            except BaseException:
                os._exit(9)
        release.set()
        thread.join(3)
        _, status = os.waitpid(pid, 0)
        adapter.close()
        queued.close()
        handler.close()
        sink.close()
        assert os.waitstatus_to_exitcode(status) == 0, status
        assert signal.getsignal(signal.SIGINT) == previous
    """)
    result: subprocess.CompletedProcess[str] = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=15
    )
    assert result.returncode == 0, result.stderr
