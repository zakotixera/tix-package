"""
Compare handler I/O and flush policies on real temporary files.
"""

from __future__ import annotations

import argparse
import json
import logging
import platform
import statistics
import tempfile
import time
from pathlib import Path

from log import FileSink, SinkHandler


def measure(
    handler: logging.Handler,
    iterations: int,
) -> float:
    """
    Time record writes and final flushing, reporting median nanoseconds per record.

    Args:
        handler: Output adapter whose lifetime is owned by the caller.
        iterations: Number of records in each of five timing repetitions.
    """
    record: logging.LogRecord = logging.LogRecord("bench", logging.INFO, "", 0, "x" * 100, (), None)
    values: list[float] = []
    _: int
    _index: int
    for _ in range(5):
        start: float = time.perf_counter()
        for _index in range(iterations):
            handler.handle(record)
        handler.flush()
        values.append((time.perf_counter() - start) * 1e9 / iterations)
    return round(statistics.median(values), 1)


def main() -> None:
    """
    Print comparable file I/O timings without asserting cross-platform thresholds.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=20000)
    args: argparse.Namespace = parser.parse_args()
    if args.iterations < 1:
        parser.error("iterations must be positive")
    results: dict[str, float] = {}
    directory: str
    with tempfile.TemporaryDirectory(prefix="tix-log-bench-") as directory:
        root: Path = Path(directory)
        handlers: dict[str, logging.Handler] = {
            "stdlib_flush_each": logging.FileHandler(root / "stdlib.log", encoding="utf-8"),
            "tix_flush_each": SinkHandler(FileSink(root / "each.log"), close_sink=True),
            "tix_flush_64": SinkHandler(
                FileSink(root / "batch.log"), flush_every=64, close_sink=True
            ),
            "tix_flush_manual": SinkHandler(
                FileSink(root / "manual.log"), flush_every=0, close_sink=True
            ),
        }
        name: str
        handler: logging.Handler
        try:
            for name, handler in handlers.items():
                results[name] = measure(handler, args.iterations)
        finally:
            for handler in handlers.values():
                handler.close()
    print(
        json.dumps(
            {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "iterations": args.iterations,
                "repeats": 5,
                "median_ns_per_record_including_flush": results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
