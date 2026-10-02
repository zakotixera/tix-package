# Validation record

Updated 2026-10-03 on Windows 11 (10.0.26200).

| Check | Result |
| --- | --- |
| `uv run pytest -q` on CPython 3.13.14 | 121 passed, 1 POSIX-only test skipped |
| `uv run --isolated --python 3.11 pytest -q` | 121 passed, 1 POSIX-only test skipped |
| `uv run mypy` | Strict checking passed for all 24 library source modules |
| `uv run ruff check .` | Passed |
| `uv run ruff format --check .` | Passed |
| `uv build --all-packages` | All three source distributions and wheels built |
| `uv run python tools/check_wheels.py` | Each wheel installed alone with `--no-deps`; no sibling imports required; `py.typed` included |
| `uv run python examples/service.py` | Completed event processing and ordered shutdown, emitted JSON records |
| Spawn process integration | Three real child processes constructed independent managers and delivered structured records to one parent listener |

Wheel checks use separate temporary virtual environments and Python isolated mode.
They verify empty runtime requirements, absent sibling installations, and unchanged
root handlers, signal handler, named logger registry, level-name registry, and thread list after import.
The standard-library asyncio module is loaded before the import snapshot because
it registers its own `asyncio` and `concurrent.futures` logger names.

Tests cover logging context precedence and task isolation, lazy filtering,
recursive field redaction, exceptions, handler ownership, sink filters, bounded
queue overflow, blocking producers, worker failures, reentrancy and concurrent close.
Event tests cover priority/sender/predicate routing, snapshot mutation, thread and
task once races, weak lifetimes, aggregate failures, cancellation draining,
context-local muting, bounded stream policies and closure. Shutdown tests cover
first causes, phases/priorities/LIFO, failure continuation, repeated and overlapping
cleanup, sync/async budgets, cancellation reports, ownership rollback, foreign
handler preservation and real subprocess signal handling. Additional regressions
cover concurrent manager closure, enqueue/flush/close deadlines and retries,
blocked-producer wakeup, fatal worker failures, nested field snapshots, atomic
rate limiting, event waiting and thread-to-loop submission, multi-loop token
notification, linked cancellation trees, cleanup dependency validation and reports
for interrupted/unattempted hooks. A deterministic clock test validates synchronous
budget exhaustion without depending on scheduler timing.

New regressions cover local TRACE naming and filtering, ordinary logger integration,
lazy file creation and modes, standard-stream redirection, buffered/severity flushing,
ownership, short writes, I/O failure reporting, concurrent writes and closure, and
inherited sink/handler rejection. Event tests exercise cache invalidation, weak
callback lifetimes, and bounded cache growth. Cancellation tests cover batched loop
notifications, cancelled futures, and waits without future allocation after cancellation.
The [performance plan](performance.md) records before/after measurements and file
I/O results; `just bench-io` reproduces the new flush-policy comparison.

The event performance follow-up adds regressions for priority ties after insertion
and replacement, exact/wildcard cache invalidation across multiple topics, releasing
strong callbacks after cancellation, and false-valued awaitables in synchronous,
serial asynchronous, and concurrent dispatch. `benchmarks/bench.py` also measures
1,000-listener subscription churn and cache reuse across 20 unrelated topics; the
performance plan links the before/after raw measurements.

CI is configured for Linux and Windows, CPython 3.11 through 3.14. That matrix has
not been executed remotely as part of this local implementation. Local checks do
not establish free-threaded Python, PyPy, or every OS signal combination as supported
configurations. Spawn fan-in is exercised locally. The POSIX fork test verifies
rejection while parent locks are held and child signal restoration; it is skipped
on Windows and has not been executed locally.

## Local performance baseline

Command: `uv run python benchmarks/bench.py --iterations 20000`. Five repeats per
case; median elapsed time divided by iteration count. Raw results are saved in
[`benchmarks/baseline-windows-py313.json`](../benchmarks/baseline-windows-py313.json).

| Operation | Median nanoseconds per call |
| --- | ---: |
| stdlib disabled debug | 63.0 |
| tix disabled debug | 355.4 |
| stdlib enabled info to NullHandler | 3377.1 |
| tix enabled info to NullHandler | 3101.4 |
| event emit with no subscribers | 426.3 |
| event emit with one subscriber | 1890.7 |
| event emit with ten subscribers | 6702.5 |
| active cancellation checkpoint | 131.2 |

These are microbenchmarks from one local run, not service-throughput measurements
or competitive rankings. In particular, the logging facade omits caller pathname
and line discovery, whereas stdlib logging performs it; its enabled timing is not
a comparison of identical functionality. The disabled facade carries extra Python
call overhead. Sink I/O, JSON serialization, slow consumers, contention, and workload
distribution can dominate real applications. There are no Rust/Go or third-party
Python performance claims and no machine-specific CI timing thresholds. Compared
with the original local implementation, explicit process checks and lifecycle/state
locks add measurable overhead: disabled logs went from approximately 172 ns to
355 ns and token checkpoints from 37 ns to 131 ns in these runs. These costs purchase
defined ownership and synchronization rather than relying on incidental GIL behavior.

Queue delivery moves I/O off producers but does not make a slow sink faster.
Backpressure policy and capacity must be selected for application load. Async
event concurrency bounds worker tasks per emission, not across all simultaneous
emissions. Signal notification uses configurable polling and cleanup deadlines
are cooperative. These limits are part of the documented API contracts.
