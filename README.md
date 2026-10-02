# Tix utility packages

**Structured logging, in-process events, and cooperative shutdown for Python.**

English | [简体中文](README.zh-CN.md)

Tix is a workspace of three independent Python 3.11+ packages with **no runtime
dependencies**. Use one on its own, or compose them through application callbacks.
Each package keeps configuration local to its instances and leaves resource
ownership, async execution, and process signals under your application's control.

| Package | Import | What it provides |
| --- | --- | --- |
| [tix-log](tix-log/README.md) | `log` | Structured fields, request context, JSON output, standard logging handlers, and optional bounded queue delivery |
| [tix-evt](tix-evt/README.md) | `evt` | Ordered sync/async dispatch, subscription filters, one-shot listeners, and bounded async streams |
| [tix-sig](tix-sig/README.md) | `sig` | Cancellation tokens, phased cleanup with dependencies, cooperative deadlines, and opt-in OS signals |

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Execution and ownership](#execution-and-ownership)
- [Examples](#examples)
- [Development](#development)
- [Documentation](#documentation)
- [License](#license)

## Installation

From the repository root, install the package you need:

```sh
python -m pip install ./tix-log
python -m pip install ./tix-evt
python -m pip install ./tix-sig
```

Each command installs one independent distribution; none requires the other two.
Distribution names use the `tix-` prefix, while Python imports use `log`, `evt`, and
`sig`. A built wheel can also be installed directly with `python -m pip install
path/to/package.whl`.

These instructions use local sources. Package names have not been checked or
reserved on PyPI. For workspace development, use [the uv setup below](#development).

## Quick start

Each example runs independently after installing its corresponding package.

### Structured logging

Attach fields to a logger, add request context, and redact sensitive fields before
writing JSON to standard error:

```python
from log import BoundLogger, JsonFormatter, LogManager, SinkHandler, StandardStreamSink, redact

handler: SinkHandler = SinkHandler(
    StandardStreamSink(),
    formatter=JsonFormatter(),
    close_sink=True,
)
manager: LogManager
with LogManager(
    "service",
    handlers=[handler],
    processors=[redact(["password", "token"])],
    close_handlers=True,
) as manager:
    logger: BoundLogger = manager.get_logger(component="api")
    with manager.context(request_id="req-1"):
        logger.info("request completed", status=200, password="secret")
```

The JSON record includes `component`, `request_id`, and `status`, with `password`
redacted. Call fields override context fields, which override bound fields.
Redaction applies to structured fields; it does not sanitize message text or
exception tracebacks. Exiting the context flushes and closes the owned handler
without closing standard error.

For files, lazy messages, `TRACE`, rate limiting, and bounded background delivery,
see the [logging guide](tix-log/README.md).

### Ordered events

Subscribe to a topic and inspect the callback's result. The subscription context
automatically disconnects the listener when it exits:

```python
from evt import DispatchResult, Event, EventBus


def confirm_order(
    event: Event,
) -> str:
    """
    Return a confirmation for the received order.

    Args:
        event: Order event whose payload is the order ID.
    """
    return f"Order {event.payload} accepted"


bus: EventBus = EventBus()
with bus.subscribe("order.created", confirm_order, priority=10):
    result: DispatchResult = bus.emit("order.created", 42)
    assert result.outcomes[0].value == "Order 42 accepted"

assert bus.listener_count == 0
```

Higher priorities run first; ties follow registration order. By default, ordinary
handler failures are grouped into an `ExceptionGroup` after all eligible handlers
run. Use `EventBus(errors="collect")` to inspect failures in the returned result.

For async handlers, await `bus.emit_async(...)`; `concurrency=N` bounds worker tasks
per emission while preserving result order. Synchronous callbacks still run inline.
The [event guide](tix-evt/README.md) also covers wildcard subscriptions, filters,
streams, waiting for one event, and dispatch from another thread.

### Cooperative shutdown

Register cleanup, request cancellation, and explicitly run shutdown:

```python
from sig import Phase, ShutdownContext, ShutdownManager, ShutdownReport


def close_resources(
    context: ShutdownContext,
) -> None:
    """
    Report resource closure for this example.

    Args:
        context: Shutdown reason, cancellation token, and remaining budget.
    """
    print(f"Closing resources: {context.reason}")


manager: ShutdownManager = ShutdownManager(timeout=5)
manager.register("resources", close_resources, phase=Phase.CLOSE)
manager.request("work complete")
assert manager.token.cancelled

report: ShutdownReport = manager.shutdown()
report.raise_errors()
```

Cleanup runs through `STOP`, `DRAIN`, and `CLOSE` phases. Within a phase, dependencies
take precedence, then higher priority and reverse registration order. Ordinary
hook failures are recorded while later hooks continue, subject to the remaining
budget. `request()` notifies cancellation; it does not execute cleanup.

Use `await manager.shutdown_async()` for async hooks. To handle OS signals, opt in
with `with manager.signals():` on the main thread and wait for a request before
running cleanup. The [shutdown guide](tix-sig/README.md) covers linked tokens,
dependency validation, deadlines, completion waits, and signal ownership.

## Execution and ownership

The packages make lifecycle decisions explicit:

| Concern | Contract |
| --- | --- |
| Import behavior | No root logging configuration or signal-handler installation. A logging worker thread starts only when constructing the optional `QueueHandler`. |
| Logging resources | Managers borrow handlers by default. Set `close_handlers=True` to transfer closure to the manager; explicitly close queues you own. Manager closure waits for accepted producer calls before flushing or closing handlers. |
| Event delivery | Dispatch uses subscription snapshots. Payloads remain caller-owned; event metadata is shallow-copied and read-only. No persistence, replay, or broker transport is provided. |
| Backpressure | Logging queues and event streams have bounded buffers with explicit overflow policies. Event streams belong to one event loop and support one consumer. |
| Async execution | No hidden event loop. Async dispatch and cleanup are awaited by the caller; blocking synchronous callbacks need an explicit adapter such as `asyncio.to_thread`. |
| Shutdown deadlines | Timeouts are cooperative. Synchronous hooks cannot be interrupted; async hooks must cooperate with cancellation. Repeated requests mark escalation without forcing process exit. |
| Processes | Live managers, tokens, buses, and sinks are process-local. Construct fresh instances in child processes and use explicit IPC for shared work or cancellation. |

See [concurrency contracts](docs/concurrency.md) for thread ownership, timeout
behavior, fork handling, and process fan-in.

## Examples

After [setting up the workspace](#development):

| Example | Run | Demonstrates |
| --- | --- | --- |
| [Service lifecycle](examples/service.py) | `just example` | Async events, contextual JSON logging, and phased shutdown composed in one finite example |
| [Multiprocessing logging](examples/process_logging.py) | `just example-process` | Fresh managers in spawned workers forwarding records to one parent listener through standard-library queues |

Both examples finish on their own. The service example shows how the three packages
work together without introducing dependencies between them.

## Development

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and
[just](https://just.systems/man/en/packages.html) **1.55+**, then run:

```sh
just setup
just check
```

`just setup` installs the workspace packages and locked development dependencies.
Windows recipes require PowerShell 7 (`pwsh`); Linux and macOS use the default POSIX
shell. Recipes work from the repository root or its subdirectories. Run `just` to
list all shortcuts.

| Command | Purpose |
| --- | --- |
| `just fmt` | Apply safe lint fixes and format Python files |
| `just check` | Check lint, formatting, strict types, and all tests |
| `just test-package evt` | Test one package; accepts `log`, `evt`, or `sig` |
| `just test-python` | Test with Python 3.11 in an isolated environment |
| `just test-python 3.14` | Test with a selected interpreter |
| `just build` | Build source distributions and wheels for all packages |
| `just wheels` | Build and check that each wheel installs independently |
| `just verify` | Run quality checks, builds, and independent wheel checks |
| `just bench 20000` | Run local microbenchmarks |
| `just bench-io 20000` | Compare file I/O flush policies |

`just test`, `just lint`, and `just types` run individual checks. To work directly
with uv, run these commands from the repository root:

```sh
uv sync --locked
uv run --locked pytest -q
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked mypy
uv build --all-packages
uv run --locked python tools/check_wheels.py
uv run --locked python examples/service.py
uv run --locked python examples/process_logging.py
uv run --locked python benchmarks/bench.py --iterations 20000
uv run --locked python benchmarks/io.py --iterations 20000
```

CI is configured for Linux and Windows with Python 3.11–3.14. See the validation
record for locally executed checks and platform limits. Benchmark results are
local measurements, with no machine-specific timing thresholds in CI.

## Documentation

| Guide | Contents |
| --- | --- |
| [Logging](tix-log/README.md) | Fields, context, processors, formatters, sinks, queue delivery, and ownership |
| [Events](tix-evt/README.md) | Subscriptions, ordering, error policies, async dispatch, streams, and thread bridges |
| [Shutdown](tix-sig/README.md) | Tokens, cleanup phases and dependencies, deadlines, reports, and OS signals |
| [Architecture](docs/architecture.md) | Design decisions, execution contracts, and scope |
| [Concurrency](docs/concurrency.md) | Thread and process contracts, lifecycle synchronization, and cooperative shutdown limits |
| [Validation](docs/validation.md) | Verification results, test coverage, platform caveats, and benchmark methodology |
| [Performance](docs/performance.md) | Measured changes, I/O tradeoffs, and follow-up priorities |
| [Event performance](docs/event-performance.md) | Dispatch and subscription optimizations with before/after measurements |
| [Reference survey](docs/research.md) | Python foundations and related Python, Rust, and Go APIs |

## License

[Apache License 2.0](LICENSE).
