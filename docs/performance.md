# Performance and feature plan

## Implemented in this iteration

| Package | Change | Purpose and contract |
| --- | --- | --- |
| log | Local `TRACE = 5`, `trace()` | Fine-grained diagnostics below DEBUG, without registering global names |
| log | `TextSink`, `StreamSink`, `StandardStreamSink`, `FileSink` | Reuse text destinations independently of records, formatters, queues, and managers |
| log | `SinkHandler` with count/severity/manual flushing | Choose latency versus buffered throughput; explicit resource ownership |
| log | Direct severity-method dispatch | Remove an intermediate call and keyword-dictionary forwarding on every log call |
| log | Avoid unnecessary message rendering in JSON formatting | Preserve existing structured messages without invoking `getMessage()` |
| evt | Reuse exact/wildcard subscription snapshots | Eliminate repeated sorting for stable subscriptions; invalidate on mutation |
| sig | Group async cancellation notifications by loop | One `call_soon_threadsafe` per token/loop instead of per waiter |
| sig | Return already-recorded cancellation causes before allocating futures | Avoid unused waiter state on repeated waits after cancellation |

The logging split is: producer context/processors → LogRecord → optional queue →
SinkHandler/formatter → TextSink → stream or file. Standard logging handlers remain
supported directly. Built-in text sinks are process-local and serialize their own
I/O; they do not create workers. Queue ownership and multi-process fan-in remain
explicit. The application owns shutdown order.

The event cache only retains topics with exact subscriptions. Unknown emitted
topics return the wildcard tuple directly, so high-cardinality publications cannot
grow the cache. Memory still scales with subscribed topics and matching wildcard
entries. Exact-topic mutations invalidate only that topic's cached tuple; wildcard
mutations invalidate all cached tuples. Ongoing dispatch keeps its existing
snapshot. No lock-free reads or incidental GIL guarantees were added.

Cancellation batches stay within each event loop. Completion still checks whether
each future is done, and all future mutations occur on the owning loop. Existing
first-cause, child-token, timeout, and concurrent cancellation contracts remain.

## Design references

- Python's [logging handlers](https://docs.python.org/3/library/logging.handlers.html)
  provide the interoperability boundary and existing rotating-file adapters.
- Python's [logging cookbook](https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes)
  describes queue/listener fan-in for multiple processes. Separate writers to one
  file are not a process-safety guarantee.
- [Loguru's logger API](https://loguru.readthedocs.io/en/stable/api/logger.html)
  provides a useful reference for TRACE at 5 and configurable destinations. Here,
  each manager owns its configuration instead of modifying a global logger.

## Measurement

Use `just bench 50000` for producer/dispatch microbenchmarks and
`just bench-io 20000` for file handler timing. Each reports a median of five repeats
with platform and interpreter metadata. File measurements include final flushing,
but not `fsync`; they write the same 100-character record through each handler.
They measure handler I/O separately from context, processors, JSON, and queues.

On Windows 11 / CPython 3.13.14, compared with initial commit `ad91060` using the
same updated producer benchmark, one local run measured:

| Operation | Before (ns/call) | After (ns/call) |
| --- | ---: | ---: |
| Disabled DEBUG | 355.8 | 291.9 |
| Enabled INFO to NullHandler | 3006.4 | 2923.6 |
| Ten subscribers, five exact and five wildcard | 7441.2 | 6711.0 |
| List those ten subscription IDs | 1158.3 | 474.6 |

These runs are indicative, not timing guarantees. No change was made to the active
cancellation checkpoint lock: it remains approximately 125 ns on this machine.
A regression test verifies that 128 waiters on one loop receive one scheduled
notification, including a waiter cancelled before delivery.

The final file benchmark measured 3853.8 ns/record for stdlib per-record flushing,
4236.1 for the new adapter with the same flush frequency, 1116.2 with a flush every
64 records, and 1071.0 with only final flushing. The adapter's process checks and
separate sink synchronization have a cost; the throughput benefit comes from
reducing flush frequency, not from claiming a universally faster stream writer.

Per-record file flushing remains the default for immediate visibility. Buffered
flushing trades visibility and potential crash loss for throughput. A sink's own
buffering, disk, antivirus, terminal, and OS page cache affect results. Prefer
workload-level measurements before selecting an interval or disabling field copies.

## Event dispatch and subscription churn follow-up

See the [event performance measurements](event-performance.md) for dispatch and
subscription churn optimizations, methodology, and raw before/after results.

## Next priorities

| Package | Candidate | Evidence needed and design constraint |
| --- | --- | --- |
| log | Batch queue draining and optional multi-record writes | Profile queue/lock costs under contention; preserve FIFO barriers, overflow accounting, partial-write errors, and close deadlines |
| log | Rotation and retention presets | Start with stdlib rotating handlers; only add a dedicated sink if measured needs require it, with one writer owning rotation |
| log | Optional faster JSON serializer | Use the existing injected `dumps` callable; benchmark realistic nested fields without introducing a mandatory dependency |
| evt | Sender-indexed subscription selection | Measure large sender-filtered fan-outs; preserve global priority/registration order and sender identity semantics |
| evt | Shared async dispatch admission limits | Define queue capacity, fairness, reentrant publish behavior, and cancellation before adding application-wide concurrency controls |
| sig | Concurrent cleanup of dependency-independent hooks | Profile slow shutdowns first; retain phases, dependency order, cooperative deadlines, and complete failure reports |
| sig | Event-driven OS signal adapters | Keep framework ownership explicit; compare platform latency against the existing portable polling adapter before using wakeup descriptors |

Add features only with a concrete workload and a testable lifecycle contract.
Keep the distributions independent; share protocols at integration boundaries
rather than introducing a mandatory common runtime package. Broaden benchmarking
to producer contention, queue saturation, many-loop cancellation, and representative
payload distributions before claiming end-to-end performance gains.
