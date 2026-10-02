# Tix utility packages

Three independent Python 3.11+ packages with no runtime dependencies:

| Directory / distribution | Import | Purpose |
| --- | --- | --- |
| [log](tix-log/README.md) / `tix-log` | `log` | Structured logging, context and bounded sink delivery |
| [evt](tix-evt/README.md) / `tix-evt` | `evt` | Ordered sync/async events and bounded streams |
| [sig](tix-sig/README.md) / `tix-sig` | `sig` | Cancellation, phased cleanup and opt-in OS signals |

Install any package directly from its directory, for example `pip install ./tix-evt`.
No distribution requires another. Names have not been checked or reserved on PyPI.

Development uses a uv workspace:

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and
[just](https://just.systems/man/en/packages.html) (1.55+). On Windows, recipes use
PowerShell 7 (`pwsh`); Linux and macOS use the default POSIX shell. Run `just` to
list the shortcuts. Recipes work from the repository root or its subdirectories.

```sh
just setup                 # Install locked development dependencies
just fmt                   # Apply safe lint fixes and formatting
just check                 # Lint, formatting, types, and all tests
just test-package evt      # Test log, evt, or sig independently
just test-python           # Test on Python 3.11 in an isolated environment
just test-python 3.14      # Select another supported interpreter
just verify                # Quality checks, builds, and independent wheel checks
just example               # Run the service example
just example-process       # Run the multiprocessing example
just bench 20000           # Run local benchmarks
just bench-io 20000        # Compare file I/O flush policies
```

Use `just test`, `just lint`, `just types`, `just build`, or `just wheels` for
individual steps. The underlying uv commands remain available for custom arguments:

```sh
uv sync
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv build --all-packages
uv run python tools/check_wheels.py
uv run python examples/service.py
uv run python examples/process_logging.py
uv run python benchmarks/bench.py --iterations 20000
```

The [reference survey](docs/research.md) collects Python foundations and modern
Python/Rust/Go APIs. The [architecture plan](docs/architecture.md) records decisions,
execution contracts and intentionally excluded features. See
[validation](docs/validation.md) for verification and performance methodology.
The [performance and feature plan](docs/performance.md) records measured changes,
I/O tradeoffs, and follow-up priorities for all three managers.

Instances never configure root logging or install signal handlers on import.
Threads start only when constructing the optional logging queue adapter.
Applications explicitly own cleanup, signal installation and async execution.

Lifecycle synchronization waits for accepted log producers before closing sinks.
Event subscriptions are separate from dispatch; streams accept the structural
`EventSource` contract. Cancellation tokens notify threads and event loops directly;
cleanup planning validates dependency graphs before executing hooks.
See [concurrency contracts](docs/concurrency.md) for ownership, timeouts, fork
behavior, process fan-in, and the remaining limits of cooperative shutdown.
