# Foundations and reference survey

Research date: 2026-10-02. Sources below are official documentation or upstream
repositories, inspected for API and design patterns, not used as benchmark evidence.
The original repository contained only empty `log/`, `evt/`, and `sig/` directories.
There were no existing features, APIs, dependency constraints, or compatibility
contracts to preserve. Distribution names are provisional until publication.

## Python foundations and dependency decisions

| Manager | Required standard library foundations | Optional ecosystem alternatives | Decision |
| --- | --- | --- | --- |
| log | `logging`, `logging.handlers`, `contextvars`, `queue`, `threading`, `json`, `traceback`, `time` | structlog for processor pipelines; Loguru for sink convenience; application-selected JSON serializers | Standard logging handlers as the sink interface; a small isolated structured facade; no mandatory backend |
| evt | `asyncio`, `weakref`, `threading`, `inspect`, `dataclasses`, `collections.abc` | Blinker for signals; pyee for emitter integration; AnyIO for multiple async runtimes | Own narrowly scoped registry to control ordering, once delivery and cancellation; asyncio execution adapter |
| sig | `signal`, `threading`, `asyncio`, `time`, `collections`, `dataclasses` | AnyIO signal receiver and cancellation scopes; application/framework lifecycle hooks | Portable opt-in signal adapter around a signal-independent cancellation and cleanup core |

Python 3.11 is the minimum for native exception groups and asyncio timeouts.
Third-party libraries below are references, not transitive dependencies. Choosing a
pure standard-library core avoids coupling independent packages to frameworks,
global registries, or a particular async runtime's cancellation model.
Development dependencies are pytest, pytest-asyncio, Ruff, and mypy; Hatchling
builds each distribution separately and uv manages the development workspace.

## Logging references

| Reference | Relevant features | Adopted design |
| --- | --- | --- |
| [Python logging handlers](https://docs.python.org/3/library/logging.handlers.html) | Rotation, network handlers, queue delivery, per-handler levels | Reuse standard handlers; explicit bounded queue policy and worker lifecycle |
| [structlog](https://www.structlog.org/en/stable/) | Bound context, dictionary processors, renderers, context variables, stdlib integration | Immutable binding facade, task-local context, ordered processors, early filtering |
| [Loguru](https://loguru.readthedocs.io/en/stable/api/logger.html) | Sink configuration, contextual fields, enqueue, filtering, lazy messages, completion | Explicit manager-owned lifecycle and lazy messages; standard rotation handlers remain usable |
| [Rust tracing](https://docs.rs/tracing/latest/tracing/) | Structured events, spans, subscribers, filtering | Separate event enrichment from output; context scopes; full tracing spans deferred |
| [Go zap](https://pkg.go.dev/go.uber.org/zap) | Structured fields, level checks, sampling, cores, sync | Avoid formatting disabled logs; injectable processors and sinks; no borrowed throughput claims |
| [Go slog](https://pkg.go.dev/log/slog) | Handler interface, attributes, derived loggers, level control | Standard handler extension seam and independently bound loggers |

## Event references

| Reference | Relevant features | Adopted design |
| --- | --- | --- |
| [Blinker](https://blinker.readthedocs.io/en/stable/) | Named and anonymous signals, sender filtering, weak receivers, return values, muting, async adapters | Arbitrary hashable topics, weak methods, sender identity, collected results, context-local muting |
| [pyee](https://pyee.readthedocs.io/en/latest/api/) | Listener registration, once listeners, async emitters, error handling, waiting for completion | Subscription handles, once claims, explicit awaited async dispatch |
| [Tokio broadcast](https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html) | Bounded broadcast storage, lag detection, receiver lifecycle | Bounded async stream per subscriber with observable drops; no claim of equivalent ring-buffer storage |
| [Go Watermill](https://watermill.io/docs/pub-sub/) | Publisher/subscriber contracts, context, acknowledgement, transport separation | Keep event envelopes and delivery policy distinct; broker acknowledgements and retries belong to external adapters |

## Shutdown references

| Reference | Relevant features | Adopted design |
| --- | --- | --- |
| [Python signal](https://docs.python.org/3/library/signal.html) | Main-thread installation and execution, platform differences, handler restoration | Explicit installation; handlers only append pending signals; application code drains requests |
| [AnyIO signals](https://anyio.readthedocs.io/en/stable/signals.html) | Async signal reception, cancellation integration | Awaitable shutdown waiting with no permanent executor thread |
| [python-graceful-shutdown](https://github.com/wbenny/python-graceful-shutdown) | Protecting startup/finalization, explicit ordering across concurrency models | Controlled cleanup lifecycle; no implicit process exit or executor shutdown |
| [Tokio shutdown](https://tokio.rs/tokio/topics/shutdown) | Detect shutdown, notify tasks, wait for completion | Request/token/cleanup separation; application explicitly joins its own tasks |
| [Rust signal-hook](https://docs.rs/signal-hook/latest/signal_hook/) | Deferred signal handling, registration lifecycle, escalation patterns | Minimal handler, reversible ownership, repeated-request escalation flag |
| [Go os/signal](https://pkg.go.dev/os/signal) and [context](https://pkg.go.dev/context) | Notification lifecycle, cancellation causes, deadlines | First reason retained, idempotent requests, cancellation tokens, monotonic cleanup budgets |

## Constraints discovered in the sources

Python signal handlers run in the main thread; taking synchronization locks there
can deadlock. The adapter therefore appends to a deque and returns. Waiting or
inspecting manager state drains it outside the handler. This intentionally trades
configurable polling latency for portability and avoids changing a framework's
process-global wakeup FD. Uncatchable signals and forcible process termination
cannot trigger cleanup. See [Python signal](https://docs.python.org/3/library/signal.html).

Context must be captured on the logging producer, before switching threads.
Async cancellation must propagate, and sibling tasks must be awaited before an
async dispatch exits. Python threads cannot be safely killed; synchronous shutdown
budgets are cooperative. See [contextvars](https://docs.python.org/3/library/contextvars.html)
and [asyncio tasks](https://docs.python.org/3/library/asyncio-task.html).
