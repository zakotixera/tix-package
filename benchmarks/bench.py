"""
Reproducible microbenchmarks; no cross-machine performance assertions.
"""

from __future__ import annotations

import argparse
import json
import logging
import platform
import statistics
import timeit
from collections.abc import Callable

from evt import ANY, Event, EventBus
from log import BoundLogger, LogManager
from sig import CancellationToken


def receive(
    event: Event,
) -> None:
    """
    Accept an event without doing application work.

    Args:
        event: Envelope delivered by the benchmark bus.
    """


def churn(
    bus: EventBus,
    topic: str,
    read_topics: tuple[str, ...] = (),
) -> None:
    """
    Add and remove one subscription, then read the selected topic snapshots.

    Args:
        bus: Registry with a stable population of subscribers.
        topic: Topic whose subscriptions change during this call.
        read_topics: Topics inspected after the mutation.
    """
    bus.subscribe(topic, receive, priority=1).cancel()
    read_topic: str
    for read_topic in read_topics:
        bus.listeners(read_topic)


def main() -> None:
    """
    Measure representative utility calls and print platform-tagged JSON timings.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=20000)
    args: argparse.Namespace = parser.parse_args()
    if args.iterations < 1:
        parser.error("iterations must be positive")
    baseline: logging.Logger = logging.Logger("baseline", logging.INFO)
    baseline.addHandler(logging.NullHandler())
    logs: LogManager = LogManager(handlers=[logging.NullHandler()])
    logger: BoundLogger = logs.get_logger()
    empty: EventBus = EventBus()
    single: EventBus = EventBus()
    single.subscribe("work", lambda event: None)
    fanout: EventBus = EventBus()
    _: int
    for _ in range(10):
        fanout.subscribe("work", lambda event: None)
    mixed: EventBus = EventBus()
    for _ in range(5):
        mixed.subscribe("work", lambda event: None)
        mixed.subscribe(ANY, lambda event: None)
    value_bus: EventBus = EventBus()
    large: EventBus = EventBus()
    for _ in range(10):
        value_bus.subscribe("work", lambda event: event.payload)
    for _ in range(1000):
        large.subscribe("work", receive, priority=_ % 21 - 10)
    scattered: EventBus = EventBus()
    topics: tuple[str, ...] = tuple(f"topic.{_}" for _ in range(20))
    topic: str
    for _ in range(5):
        scattered.subscribe(ANY, receive)
        for topic in topics:
            scattered.subscribe(topic, receive)
    for topic in topics:
        scattered.listeners(topic)
    token: CancellationToken = CancellationToken()
    cases: dict[str, Callable[[], object]] = {
        "stdlib_disabled": lambda: baseline.debug("disabled"),
        "tix_log_disabled": lambda: logger.debug("disabled"),
        "stdlib_null_sink": lambda: baseline.info("enabled"),
        "tix_log_null_sink": lambda: logger.info("enabled"),
        "evt_empty": lambda: empty.emit("work"),
        "evt_one": lambda: single.emit("work"),
        "evt_ten": lambda: fanout.emit("work"),
        "evt_ten_mixed": lambda: mixed.emit("work"),
        "evt_ten_values": lambda: value_bus.emit("work", 42),
        "evt_mixed_snapshot": lambda: mixed.listeners("work"),
        "evt_churn_1000": lambda: churn(large, "work"),
        "evt_churn_unrelated_20_topics": lambda: churn(scattered, "temporary", topics),
        "sig_checkpoint_active": token.checkpoint,
    }
    measurements: dict[str, dict[str, float]] = {}
    name: str
    callback: Callable[[], object]
    for name, callback in cases.items():
        values: list[float] = timeit.repeat(callback, number=args.iterations, repeat=5)
        measurements[name] = {
            "median_ns_per_call": round(statistics.median(values) * 1e9 / args.iterations, 1)
        }
    print(
        json.dumps(
            {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "iterations": args.iterations,
                "repeats": 5,
                "cases": measurements,
            },
            indent=2,
        )
    )
    logs.close()


if __name__ == "__main__":
    main()
