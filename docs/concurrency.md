# Concurrency and process ownership

The packages remain independently installable. Their small private process/deadline
helpers are intentionally local: sharing these helpers through a fourth runtime
package would introduce a deployment dependency for little reusable behavior.

## Responsibilities

| Component | Owns | Does not own |
| --- | --- | --- |
| Log facade | Bound/context fields and processors | Process-global logging configuration |
| Logging delivery | Producer leases, level changes, handler close ordering | User callbacks' internal synchronization |
| Logging queue | Bounded record backlog, worker, barriers, optional sink close | Killing blocked I/O or sharing across processes |
| Event registry | Subscription indexing, weak lifetime, once claims | Callback execution or event-loop lifetime |
| Event bus | Sync/async invocation, cancellation draining, result policy | Payload mutation or broker persistence |
| Event stream | One loop and bounded consumer buffer | Cross-thread asyncio queue mutation |
| Cancellation token | First cause, linked children, thread/multi-loop notification | OS signal installation or process termination |
| Hook planner/runner | Dependency validation, ordering, budgets, outcomes | Manager lifecycle or global signal ownership |
| Shutdown manager | Request/cleanup lifecycle and cached completion | Application task creation and joining |
| Signal adapter | Explicit, reversible OS registration and deferred requests | Framework event-loop policy or wakeup descriptor |

## Thread contracts

Logging delivery registers each accepted call under a condition before releasing
the lock to execute processors and sinks. Close stops acceptance and waits for
leases; callback reentry into lifecycle operations fails immediately. Level changes
and queue statistics are synchronized. Processors may run concurrently; stateful
custom processors must synchronize their own state. `RateLimit` does so internally.
Leases have monotonically increasing IDs: flush waits for earlier leases before
flushing handlers, without waiting for later producers or blocking recursive log
calls. Flush itself holds a lease so concurrent close cannot overtake it. Handler
cleanup collects all failures, including interruptions, attempts remaining cleanup,
then raises an `ExceptionGroup` or `BaseExceptionGroup` as appropriate.

Queue producers never hold the sink lock while waiting for space. Closing wakes
blocked producers, and timeout does not silently discard accepted records. A worker
failure records its cause, drops remaining undeliverable records with counters,
and wakes waiters. Barriers abandoned by a timed-out flush are removed when they
are still queued. Deep copies protect accepted structured values from subsequent
caller mutation; they cannot protect against concurrent mutation during copying.

Registry callbacks run outside its lock. Dispatch still uses snapshot semantics.
Weak cleanup checks process ownership before touching registry state. Async streams
remain loop-local; use `submit` to marshal publication. Event `wait_for` uses a loop
future and a temporary subscription rather than a background executor waiter.

Cancellation transitions copy waiter/child references under a lock, then notify
outside it. Loop futures are completed only on their owning loops. Linked child
cancellation is iterative, avoiding recursion-limit failure for deep trees. Tokens
are process-local. Shutdown OS polling remains separate because Python signal
handlers must not acquire synchronization locks.

## Process contracts

Live managers and their locks cannot be transferred to another process. PID guards
run before inherited lock access; callers must construct new instances in children.
In a fork child, queue flush/close do nothing so Python's logging shutdown cannot
wait on a vanished parent worker. Signal installation registers a child callback
that restores only the handlers still owned by the adapter. It does not reset
arbitrary application locks, adopt inherited workers, or make multithreaded fork
generally safe.

Prefer a caller-selected `spawn` or `forkserver` context. The library never calls
`set_start_method`. [Python multiprocessing documentation](https://docs.python.org/3/library/multiprocessing.html)
explains context compatibility and the hazards of forking multithreaded processes.

The [process logging example](../examples/process_logging.py) uses established
stdlib queue fan-in: fresh managers and `logging.handlers.QueueHandler` in children,
one parent `QueueListener` and sink, and explicit feeder draining. Cross-process
fields must be pickleable; never connect multiprocessing's own diagnostic logger
to its internal queue. See the [logging cookbook](https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes).

No shared-memory event bus, automatic worker termination, or process-global
cancellation singleton is supplied. Use a caller-owned `multiprocessing.Event`,
pipe, queue, or external broker when state must cross process boundaries.

## Practical limits

Bounded queue capacity limits waiting records, excluding the one being processed
and concurrent callers' control barriers. Timeouts release callers; they cannot
kill a Python thread or force a sink to return. Manager close waits for its accepted
callbacks, so blocking direct sinks still need application-configured I/O deadlines.
Do not build cyclic dependencies where a callback waits for the thread closing it.

Async cancellation and cleanup budgets remain cooperative. Cross-thread submission
requires a running loop whose lifecycle outlasts the returned future. Cancellation
of that future requests cancellation; it is not proof that the coroutine has
finished its finally blocks. Observe application completion when that distinction
matters. Free-threaded Python and alternative interpreters are not yet validated.
