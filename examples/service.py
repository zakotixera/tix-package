"""
Finite example: independent managers compose through application callbacks.
"""

from __future__ import annotations

import asyncio

from evt import Event, EventBus
from log import (
    BoundLogger,
    JsonFormatter,
    LogManager,
    QueueHandler,
    SinkHandler,
    StandardStreamSink,
)
from sig import Phase, ShutdownManager, ShutdownReport


async def main() -> None:
    """
    Process one event and shut down the independently composed managers.
    """
    sink: SinkHandler = SinkHandler(
        StandardStreamSink(), formatter=JsonFormatter(), close_sink=True
    )
    queued: QueueHandler = QueueHandler(sink, close_sink=True)
    logs: LogManager
    with LogManager("example", handlers=[queued], close_handlers=True) as logs:
        logger: BoundLogger = logs.get_logger(component="service")
        bus: EventBus = EventBus()
        shutdown: ShutdownManager = ShutdownManager(timeout=5)

        async def process(
            event: Event,
        ) -> None:
            """
            Log the work item delivered to the example subscriber.

            Args:
                event: Event being transformed, selected, or delivered.
            """
            logger.info("processed", item=event.payload)

        bus.subscribe("work", process)
        shutdown.register("log flush", lambda ctx: logs.flush(), phase=Phase.CLOSE)
        shutdown.register("subscriptions", lambda ctx: bus.clear(), phase=Phase.DRAIN)
        shutdown.register(
            "notice", lambda ctx: logger.info("stopping", reason=ctx.reason), phase=Phase.STOP
        )
        with shutdown.signals():
            try:
                with logs.context(request_id="example-1"):
                    await bus.emit_async("work", {"id": 1})
                shutdown.request("example complete")
                await shutdown.wait_async()
            finally:
                report: ShutdownReport = await shutdown.shutdown_async()
        report.raise_errors()


if __name__ == "__main__":
    asyncio.run(main())
