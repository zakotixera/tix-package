"""
Spawn-safe fan-in: each child owns managers, one parent owns the output sink.
"""

from __future__ import annotations

import io
import logging
import logging.handlers
import multiprocessing
import multiprocessing.queues

from evt import EventBus
from log import JsonFormatter, LogManager, execution_context
from sig import ShutdownManager


def worker(
    records: multiprocessing.queues.Queue[logging.LogRecord],
    identifier: int,
) -> None:
    # Only the multiprocessing queue crosses the boundary. Create live managers here.
    """
    Process one work item using managers created inside this child process.

    Args:
        records: Multiprocessing queue transporting pickleable log records.
        identifier: Identifier of the subscription or worker.
    """
    handler: logging.handlers.QueueHandler = logging.handlers.QueueHandler(records)
    logs: LogManager
    with LogManager(
        "worker", handlers=[handler], close_handlers=True, processors=[execution_context]
    ) as logs:
        bus: EventBus = EventBus()
        shutdown: ShutdownManager = ShutdownManager()
        bus.subscribe("job", lambda event: logs.get_logger().info("processed", job=event.payload))
        bus.emit("job", identifier)
        shutdown.register("events", lambda ctx: bus.clear())
        shutdown.shutdown("done").raise_errors()
    # Flush this child's queue feeder before its process exits.
    records.close()
    records.join_thread()


def main() -> None:
    """
    Spawn three independent workers and collect their logs through one parent sink.
    """
    context: multiprocessing.context.SpawnContext = multiprocessing.get_context("spawn")
    records: multiprocessing.queues.Queue[logging.LogRecord] = context.Queue()
    output: io.StringIO = io.StringIO()
    sink: logging.StreamHandler[io.StringIO] = logging.StreamHandler(output)
    sink.setFormatter(JsonFormatter())
    listener: logging.handlers.QueueListener = logging.handlers.QueueListener(records, sink)
    children: list[multiprocessing.context.SpawnProcess] = [
        context.Process(target=worker, args=(records, index)) for index in range(3)
    ]
    listener.start()
    try:
        child: multiprocessing.context.SpawnProcess
        for child in children:
            child.start()
        for child in children:
            child.join(10)
            if child.exitcode != 0:
                raise RuntimeError(f"Worker failed or timed out: {child.exitcode}")
    finally:
        for child in children:
            if child.is_alive():
                child.terminate()
                child.join(3)
        listener.stop()
        records.close()
        records.join_thread()
        for child in children:
            child.close()
    print(output.getvalue(), end="")


if __name__ == "__main__":
    main()
