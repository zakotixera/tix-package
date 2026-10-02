# Development shortcuts; uv owns environments and dependencies.
set minimum-version := "1.55.0"

[windows]
set shell := ["pwsh", "-NoLogo", "-NoProfile", "-Command"]

# List available development commands.
default:
    @just --list

# Install workspace packages and development tools from the lockfile.
setup:
    uv sync --locked

# Apply safe lint fixes and format Python files.
fmt:
    uv run --locked ruff check --fix .
    uv run --locked ruff format .

# Check lint and formatting without changing files.
lint:
    uv run --locked ruff check .
    uv run --locked ruff format --check .

# Run strict type checking for all libraries.
types:
    uv run --locked mypy

# Run the full test suite.
test:
    uv run --locked pytest -q

# Run one package's tests: log, evt, or sig.
[arg("package", pattern="log|evt|sig")]
test-package package:
    uv run --locked pytest -q tix-{{ package }}/tests

# Run tests on a Python version in an isolated environment (default: minimum supported).
[arg("version", pattern='3\.(11|12|13|14)(\.[0-9]+)?')]
test-python version="3.11":
    uv run --locked --isolated --python {{ version }} pytest -q

# Run lint, formatting, types, and tests.
check: lint types test

# Build source distributions and wheels for all packages.
build:
    uv build --all-packages

# Build and verify that each wheel installs independently.
wheels: build
    uv run --locked python tools/check_wheels.py

# Run all quality checks and independent package build/install checks.
verify: check wheels

# Run the finite service composition example.
example:
    uv run --locked python examples/service.py

# Run the spawn-process logging example.
example-process:
    uv run --locked python examples/process_logging.py

# Run local microbenchmarks with the requested iteration count.
[arg("iterations", pattern="[1-9][0-9]*")]
bench iterations="20000":
    uv run --locked python benchmarks/bench.py --iterations {{ iterations }}

# Compare file handler throughput with different flush policies.
[arg("iterations", pattern="[1-9][0-9]*")]
bench-io iterations="20000":
    uv run --locked python benchmarks/io.py --iterations {{ iterations }}
