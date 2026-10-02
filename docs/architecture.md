# Architecture and implementation plan

## Shared boundaries

Three independent src-layout distributions expose `log`, `evt`, and
`sig`. None imports another. Composition happens in the application through
callables and standard handlers. Imports perform no configuration, install no
signals, and start no threads. Instances own their state; no application singleton
is supplied. Public models are typed, and wheels include `py.typed`.

Implementation order: packaging and contracts; logging pipeline and queue;
event registry and async adapters; cancellation core and signal adapter; behavioral
tests; installed-wheel checks, static checks, examples and reproducible benchmarks.

## log: structured facade over standard sinks

`LogManager` owns configuration and an isolated `logging.Logger`, outside the
global logging registry. `BoundLogger` holds copied fields and shares the manager.
The hot path checks the manager's level before evaluating lazy messages, merging
context, allocating records or running processors. Fields merge in this order:
bound values, context values, call values. Envelope keys are reserved.

Processors transform an event dictionary or return `None` to drop it. Filters,
redaction and application-specific sampling can therefore remain ordinary
callables. JSON formatting uses a replaceable serializer. Standard handlers supply
console, files, rotation, network delivery and formatter/filter customization.

Optional `QueueHandler` is a standalone bounded delivery adapter. Full queues
either block, drop the new record, or raise. Counters expose rejected records and
sink failures. Flush inserts a barrier; close rejects new records, drains accepted
work, flushes/optionally closes its sink on the worker, and joins it. No worker callback may
reenter that queue. Manager close is explicit and idempotent; sinks are borrowed
unless `close_handlers=True`. Queue wrappers are always closed by their caller.

An internal `Delivery` component owns producer leases and close coordination,
separate from field enrichment. Close waits for accepted producers; user callbacks
run outside lifecycle locks. Queue waits have explicit deadlines, closing wakes
blocked producers, and fatal worker failures wake observers. Nested queued event
fields are deep-copied by default. Reusable `RateLimit` and `execution_context`
processors provide volume control and producer identity.

Performance targets are structural: disabled calls skip enrichment; sink I/O can
leave the producer; storage is bounded when configured. This is not a claim that a
Python facade outperforms Rust/Go or direct stdlib logging.

## evt: registry, dispatch and streams

An internal `Registry` maps hashable exact topics to immutable, priority-sorted subscription
tuples. Mutation replaces tuples under a lock; callbacks run outside that lock.
The `ANY` sentinel subscribes to all topics without string-pattern scans. Higher
priority runs first, then registration order. Payloads are passed in immutable
envelopes; payload contents remain caller-owned. Sender filters use identity.

Handles disconnect deterministically or act as context managers. Weak references
are opt-in. A once subscription is claimed atomically immediately before invocation
after filters pass, including under concurrent/reentrant publish. Dispatch uses
snapshot semantics: ordinary subscriptions removed during a dispatch can still
run in that dispatch. New subscriptions appear on the next publish.

Sync emit rejects awaitables instead of starting hidden loops. Async emit accepts
both sync and async handlers; sync handlers execute inline and must be nonblocking.
Sequential execution preserves completion order. Concurrent execution uses a fixed
number of worker tasks and preserves result ordering. Ordinary exceptions either
become result entries or an ExceptionGroup after all eligible callbacks run;
cancellation and BaseException escape.
Every concurrent worker is cancelled and awaited when dispatch is cancelled.

An async stream is an optional bounded subscription adapter with drop-oldest,
drop-newest or raise policies and a drop counter. It belongs to the event loop where
it was created; cross-thread publishers must marshal through that loop. Closing
disconnects it and wakes its single consumer. Bus muting uses context variables.
Streams accept the structural `EventSource` protocol instead of a concrete bus.
`wait_for` owns a temporary subscription and disconnects on timeout/cancellation;
`submit` uses asyncio's thread-safe submission to marshal delivery onto a target loop.

This is an in-process notification bus, without persistence, broker transport,
automatic retries, schema validation, replay or exactly-once business guarantees.

## sig: cancellation, cleanup and OS ownership

`CancellationToken` exposes first-cause cancellation, sync/async waits and explicit
checkpoints. `ShutdownManager` has an active -> requested -> running -> closed
lifecycle and a separate escalation flag. Requests are idempotent, preserve their
first reason, and repeated requests mark escalation without terminating the process.
Token waits are event-driven across threads and loops; linked child tokens propagate
parent cancellation downward. Waiting for OS signals retains a bounded polling timer.

Hooks receive a `ShutdownContext` and run by ascending phase, descending priority,
then reverse registration order subject to explicit `after` dependencies. An internal
planner validates the dependency graph before state changes; a separate runner owns
sync/async invocation and outcome collection. This provides stop-accepting, drain and close
phases and stack-like cleanup within each priority. Registration freezes once
cleanup starts. Hook failures are collected while later hooks continue. A completed
report is cached; overlapping cleanup is rejected rather than silently duplicated.
Independent observers use sync/async completion waits. Interrupted reports identify
both the interrupted hook and all unattempted hooks.

Async cleanup supports per-hook and overall cooperative deadlines. Sync cleanup
checks budgets between hooks and reports overruns; it never claims to preempt a
blocking function. Async cancellation propagates and marks the manager closed so
partly completed cleanup cannot silently rerun. Cleanup does not require OS signals.

`SignalAdapter` installs only explicitly requested catchable signals, defaults to
SIGINT/SIGTERM, enforces the main thread, rolls back partial installation, and
rejects another adapter's ownership. Restoration only touches handlers it still
owns. The handler queues the signal number; `poll`, `wait`, `wait_async` and cleanup
drain pending signals and notify the token outside signal context. Installation is
separate from cleanup and is scoped with a context manager. No `atexit`, implicit
`sys.exit`, global event-loop policy changes, or automatic hard-kill behavior.
Signal storms coalesce into two pending requests and polling is serialized. Explicit
installation registers a POSIX child-after-fork restoration hook. All live managers
check process ownership before touching inherited locks; applications construct fresh
instances in worker processes. See [concurrency contracts](concurrency.md).

## Verification and future extensions

Behavioral tests cover queue pressure/draining/failures, context isolation, no global
logger mutation, event ordering/weak lifetime/once races/reentrancy/cancellation,
stream closure, cleanup ordering/deadlines/repeated requests, signal restoration,
and subprocess signal delivery. CI spans Windows and Linux, Python 3.11-3.14.
An integration example composes the packages without introducing dependency edges.
Benchmarks report local timings with interpreter/platform metadata and baseline
comparisons; they impose no machine-specific pass thresholds.

Future adapters may add tracing spans, external serialization, AnyIO dispatch,
or broker transport. They are deliberately outside this
initial implementation; existing extension points support application-specific
behavior without a shared framework or oversized base manager.
