# Event dispatch and subscription performance

Profiling the current workspace identified repeated `inspect.isawaitable(None)`
calls in dispatch. Both synchronous and asynchronous dispatch now skip that check
for `None`; all other values still use the standard awaitability check, including
false-valued custom awaitables. `None` remains a recorded callback outcome.

Subscription insertion uses standard-library binary search instead of sorting the
whole topic again. Equal priorities retain registration order. Copying the immutable
topic snapshot and removing a subscription still take linear time in topic size.
Exact-topic mutations now invalidate only that topic's merged snapshot, so unrelated
subscription churn does not force other topics to repeat wildcard merging.

The following measurements compare the working tree immediately before and after
this follow-up, including the existing snapshot cache in both runs. Command:
`uv run --locked python benchmarks/bench.py --iterations 10000`. Each value is the
median of five repeats on Windows 11 / CPython 3.13.14, without profiling enabled.

| Operation | Before (ns/call) | After (ns/call) | Time reduction |
| --- | ---: | ---: | ---: |
| One subscriber returning `None` | 1741.3 | 1441.1 | 17% |
| Ten subscribers returning `None` | 6591.6 | 4251.4 | 35% |
| Ten mixed exact/wildcard subscribers returning `None` | 6707.7 | 4421.2 | 34% |
| Ten subscribers returning integers | 6542.3 | 6432.1 | 2% |
| Add/remove a subscription among 1,000 listeners | 65578.8 | 24173.4 | 63% |
| Mutate an unrelated topic, then list 20 mixed-topic snapshots | 27221.4 | 12235.0 | 55% |

Raw [before](../benchmarks/performance-before-windows-py313.json) and
[after](../benchmarks/performance-after-windows-py313.json) results include unchanged
logging and cancellation controls. The churn case uses 1,000 permanent listeners
spread across 21 priorities. The 20-topic case uses five exact listeners per topic
and five wildcard listeners, with caches warmed before measurement. Each mutation
adds and removes one listener. These are local microbenchmarks, not application
throughput guarantees; the small difference for integer-returning handlers is
within ordinary run-to-run variation. No timing thresholds are enforced in tests.
