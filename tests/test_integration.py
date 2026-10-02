from __future__ import annotations

import io
import json
import logging

from evt import EventBus
from log import JsonFormatter, LogManager, QueueHandler
from sig import Phase, ShutdownManager, ShutdownReport


async def test_independent_managers_compose() -> None:
    """
    Verify independent managers compose.
    """
    output: io.StringIO = io.StringIO()
    sink: logging.StreamHandler[io.StringIO] = logging.StreamHandler(output)
    sink.setFormatter(JsonFormatter())
    queued: QueueHandler = QueueHandler(sink)
    logs: LogManager = LogManager(handlers=[queued], close_handlers=True)
    bus: EventBus = EventBus()
    shutdown: ShutdownManager = ShutdownManager()
    bus.subscribe("stop", lambda event: shutdown.request(event.payload))
    shutdown.register("logs", lambda ctx: logs.close(), phase=Phase.CLOSE)
    shutdown.register("bus", lambda ctx: bus.clear(), phase=Phase.DRAIN)
    shutdown.register(
        "notice",
        lambda ctx: logs.get_logger().info("stopping", reason=ctx.reason),
        phase=Phase.STOP,
    )
    try:
        await bus.emit_async("stop", "finished")
        assert await shutdown.wait_async() == "finished"
        report: ShutdownReport = await shutdown.shutdown_async()
        report.raise_errors()
        assert bus.listener_count == 0
        assert json.loads(output.getvalue())["reason"] == "finished"
    finally:
        logs.close()
        queued.close()
