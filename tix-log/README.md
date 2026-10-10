# tix-log

An isolated structured logging facade over Python's standard `logging.Handler`.
Python 3.11+, no runtime dependencies. Install with `pip install ./tix-log` from the
repository root, or install a built wheel.

```python
from log import JsonFormatter, LogManager, QueueHandler, SinkHandler, StandardStreamSink, redact

sink = SinkHandler(StandardStreamSink(), formatter=JsonFormatter(), close_sink=True)
queue = QueueHandler(sink, capacity=1024, overflow="drop", close_sink=True)
with LogManager(
    "service", handlers=[queue], close_handlers=True, processors=[redact(["password", "token"])]
) as manager:
    log = manager.get_logger(component="api")
    with manager.context(request_id="req-1"):
        log.info("request completed", status=200, password="secret")
    log.debug(lambda: "expensive message is only evaluated when enabled")
print(queue.stats)
```

`get_logger(**fields)`, `bind(**fields)` and `unbind(*keys)` create immutable field
bindings (nested values remain caller-owned). `context(**fields)` restores nested
scopes and isolates asyncio tasks. Call fields override context fields, which
override bound fields. `message`, `level`, `logger`, `timestamp` and `exception` are
reserved envelope keys. Timestamp is Unix seconds. `log.exception(...)` attaches
the current exception; `log.log(level, message, exc_info=True)` also works.

`LogManager(level=..., handlers=..., processors=..., close_handlers=False)` copies
configuration. Each processor receives a mutable event dict, returning a dict or
`None` to drop it. Processors must preserve envelope fields needed by later stages.
Processor and direct sink exceptions propagate. Return values indicate whether an
event reached delivery, not whether a queue accepted it or every sink wrote it.
`enabled(level)` and `set_level(level)` support runtime early filtering. Disabled
messages avoid lazy evaluation and processor work. Closed managers reject logging.

`log.levels` defines all supported levels, also exported directly from `log`:
`NOTSET = 0`, `TRACE = 5`, `DEBUG = 10`, `INFO = 20`, `WARNING = 30`, `ERROR = 40`,
and `CRITICAL = 50`. Standard levels reuse Python's logging constants, so either
import works with managers and standard handlers.

`TRACE = 5` is below `DEBUG = 10`. Enable it with `LogManager(level=TRACE)`
or `manager.set_level(TRACE)`, then call `log.trace(message, **fields)` or
`log.log(TRACE, message)`. Trace records carry `levelname="TRACE"` and structured
`level="trace"`. Importing the package does not register a global logging level or
modify `logging.Logger`. Use numeric `TRACE` for standard handler thresholds;
`logging.getLevelName(TRACE)` remains controlled by the application. All severity
convenience methods accept `exc_info=True`.

## Text destinations and flushing

`SinkHandler` adapts the `TextSink` protocol (`write(text)`, `flush()`, `close()`) to
standard logging. Sinks know nothing about records, formatting, queues, or managers
and can also be used directly. The adapter accepts any standard formatter and
filter; its default formatter writes message text. Custom sinks must implement
complete writes or raise, flush during close, and synchronize any shared use.

| Sink | Behavior |
| --- | --- |
| `StreamSink(stream, close_stream=False)` | Inject a text stream; borrow it by default |
| `StandardStreamSink("stderr")` | Resolve current `sys.stderr` on each operation; `"stdout"` is also supported; never close standard streams |
| `FileSink(path, ...)` | Lazily open and own a buffered text file; UTF-8 and append mode by default |

`FileSink` accepts `mode="a"`, `"w"`, or `"x"`, `encoding`, `errors`, `buffering`,
and `create_parents=False`. Paths are anchored at construction; only the first
write opens the file or creates optional parents. Flushing or closing an unused
sink does not create files. Output uses `\n` on every platform. Writes after close
raise and cannot reopen or truncate the file. Explicitly close directly used sinks.

```python
from log import TRACE, FileSink, JsonFormatter, LogManager, SinkHandler

handler = SinkHandler(
    FileSink("logs/service.jsonl", create_parents=True),
    formatter=JsonFormatter(),
    flush_every=64,
    close_sink=True,
)
with LogManager(level=TRACE, handlers=[handler], close_handlers=True) as manager:
    logger = manager.get_logger(component="worker")
    logger.trace("job dequeued", job_id=42)
    logger.error("job failed", job_id=42)  # Flush immediately at ERROR and above.
```

`SinkHandler(..., level=NOTSET, formatter=None, terminator="\n", flush_every=1,
flush_level=ERROR, close_sink=False)` controls rendering and flush policy.
`flush_every=0` disables count-based flushing; `flush_level=None` disables severity
flushing. Explicit `flush()` and `close()` still flush. A flush resets the record
counter. Increasing the interval reduces flush operations but delays visibility;
there is no background timer. A crash can lose buffered output. Flush drains Python
buffers, **not** an `fsync` durability guarantee. A stream's own line-buffering or
unbuffered configuration can still force earlier writes.

Built-in sinks serialize complete writes, flushes, and closure per instance;
`SinkHandler` additionally serializes rendering and record-count decisions.
The lock only coordinates users of that instance, not separate sinks, external
writers, processes, or concurrent reassignment of `sys.stdout`/`sys.stderr`.
Late-bound standard streams are best used with the default per-record flushing;
for buffered redirected output, inject and retain an explicit `StreamSink`.
Missing standard streams, short writes, encoding errors, and I/O failures propagate
to direct callers; `QueueHandler` records them in its failure statistics.
Owned resources are closed even if a final stream flush fails. Configure resources
before sharing them across threads. Recreate sinks and handlers in child processes;
inherited writes fail before touching locks, while inherited cleanup is a no-op.

`JsonFormatter(dumps=...)` accepts a serializer returning `str`, and also formats
ordinary standard-library records. Exception tracebacks are rendered separately;
structured exceptions are captured on the producer before queue transport, so
standard-library queue preparation and process serialization preserve them.
When `exc_info` remains available, `JsonFormatter` calls its own `formatException()`
so subclass rendering and redaction apply. Captured text is used when transport
has removed the original exception information.
Field redaction does **not** sanitize free-text messages or tracebacks. Use
`logging.handlers.RotatingFileHandler`, `TimedRotatingFileHandler`, custom formatters, filters and
other standard handlers directly. Caller pathname/line capture is intentionally
omitted from the facade's hot path; use structured location fields if needed.

`QueueHandler(sink, capacity=1024, overflow="drop", enqueue_timeout=None,
copy_fields=True, close_sink=False)` uses one daemon worker. Policies: `block`
applies producer backpressure, `drop` drops new records, `raise` raises `queue.Full`.
A blocking enqueue raises `queue.Full` when its timeout expires. `stats` is a locked
snapshot of accepted, dropped/rejected, failed and queued counts. `last_error`
retains the latest sink failure. Ordinary exceptions do not stop the worker; a
fatal `BaseException` closes delivery and subsequent operations raise RuntimeError.
The sink must not be another Tix `QueueHandler`: direct queue nesting is rejected
at construction before another worker starts. Use one queue per delivery path.

`flush(timeout=None)` waits for earlier accepted records and a sink flush.
`close(timeout=None)` stops acceptance, wakes blocked producers, drains accepted
records and joins the worker. Timeout raises `TimeoutError` without killing a sink;
close can be retried. The worker owns final sink flush and optional sink closure.
No sink may reenter its own queue or close its owning manager. Configure sink I/O
timeouts as well. Flush/close after a fatal worker failure report that failure.
Sink exceptions already recorded in `last_error` are not replayed as producer errors.

Structured fields are deep-copied before enqueue by default, and `%` message
arguments are resolved on the producer. Custom values must support deepcopy; its
errors propagate. `copy_fields=False` opts into shallow copies and requires callers
to keep nested values immutable. Never mutate fields concurrently with a log call.

Managers borrow handlers by default: `close()` flushes but only closes them with
`close_handlers=True`. Queue owners must explicitly close their queues, or give
that responsibility to a manager. Manager `flush()` waits for producer calls
accepted before its barrier, including their processors, before flushing handlers.
Later producers can continue; they do not extend the producer barrier. Concurrent
flushes are ordered, and close waits for accepted flush operations as well.
Manager close rejects new work and waits for
accepted producer calls before flushing/closing sinks; concurrent closers wait for
the same completion. Processor and sink callbacks run outside lifecycle locks.
Cleanup attempts every handler's flush and owned close even when an operation
raises `KeyboardInterrupt`, `SystemExit`, or another `BaseException`. Failures are
reported together afterward: `ExceptionGroup` for ordinary errors, otherwise
`BaseExceptionGroup`. Closure means all cleanup operations were attempted, not that
every destination successfully released its resources; failed cleanup is not retried.
Recursive flush/close from those callbacks raises instead of deadlocking. Arbitrary
borrowed handlers still need their own safe implementation and I/O deadlines.

`execution_context` adds producer process/thread IDs and thread name. `RateLimit(rate,
burst=1, capacity=1024, key=None)` is a thread-safe processor with per-key token
buckets and bounded LRU storage. The default key is `(logger, message)`; `dropped`
reports filtered events. New/evicted keys start with a full burst, so use this for
log volume control, not security limits. Place redaction before stateful processors
when keys could retain secrets.

All managers, stateful processors and queue adapters belong to their creating
process. Reusing inherited state raises before touching locks. Inherited queue
`flush`/`close` are harmless no-ops for standard logging shutdown. For multiprocessing
fan-in, construct a stdlib `logging.handlers.QueueHandler` in each child and one
`QueueListener` in the parent, as in [the spawn example](../examples/process_logging.py).
Never share a live manager or file sink between processes.
