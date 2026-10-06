set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    # The benchmark package is left out of the all-features legs on purpose: it is built with the
    # feature set a service ships, and the framework's harness feature is a compile error in it.
    # Its own leg follows.
    cargo clippy --workspace --exclude ruststream-kinesis-bench --all-targets --all-features -- -D warnings
    cargo clippy -p ruststream-kinesis-bench --all-targets -- -D warnings
    cargo check --workspace --exclude ruststream-kinesis-bench --all-targets --all-features
    cargo check -p ruststream-kinesis-bench --all-targets
    cargo check --workspace --no-default-features

test:
    cargo test --workspace --all-features

brokers-up:
    docker compose -f docker-compose.test.yml up -d --wait

brokers-down:
    docker compose -f docker-compose.test.yml down -v

test-brokers: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    # This recipe starts the stand, so a gated test that skips itself here is a fault, not a
    # developer without a broker.
    KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
    RUSTSTREAM_REQUIRE_LIVE=1 \
        cargo test --workspace --all-features -- --test-threads=1

# What this crate costs over the aws-sdk-kinesis client it wraps: two scenarios run as a RustStream
# service and as a hand-written loop, against the stack the tests use. On demand only - it takes
# minutes and it wants the machine to itself. The page it feeds is docs/benchmarks.md.
bench *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    mkdir -p target
    # RUSTFLAGS is cleared so the numbers are not tied to this machine's CPU: a binary built with
    # `-C target-cpu=native` cannot be reproduced anywhere else.
    RUSTFLAGS="" KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
    RUSTSTREAM_BENCH_OUT="$PWD/target/bench-paired.json" \
        cargo bench -p ruststream-kinesis-bench --bench paired {{ ARGS }}
    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json

# What this crate's own code costs per message, counted under valgrind: instructions through
# callgrind and allocations through DHAT, each scenario a service on the production broker against
# the stack the tests use, with everything the service's thread runs counted, the AWS SDK's
# requests included. It takes minutes, and the counts barely move with the machine's load, so it
# needs the stack but not a quiet machine. The page it feeds is the code table of
# docs/benchmarks.md. RUSTFLAGS is cleared because valgrind aborts on the instructions a recent CPU
# advertises. Needs valgrind.
#
# The benchmarks hand the measurement to gungraun's runner, which has to be the release of the
# library the lock file pins. The recipe installs that release into `target/gungraun-runner` on
# the first run and after the library moves, and puts it first on PATH, where the benchmarks look
# the runner up. A `GUNGRAUN_RUNNER` in the environment would win over PATH when the benchmarks
# build, so the recipe clears it.
#
# A leading number is the deliveries per measured run: the default of 1000 is what the published
# document is measured at, a larger count buys a steadier number for a longer run
# (`just bench-code 2000`). The benches read it at build time, so a new count rebuilds them. The
# other arguments reach the runner: `just bench-code --save-baseline=main` records a baseline,
# `just bench-code --baseline=main` measures against it. Totals over another count are not
# comparable, so each count keeps its runs and baselines in a directory of its own,
# `target/gungraun/<count>`.
#
# A run against a baseline, named with `--baseline` or in `GUNGRAUN_BASELINE`, fails on two
# percent more instructions than the baseline in a scenario. The limit is relative, so it applies
# only there: a plain run would be held to whatever run came before it, of whatever tree. The
# allocation limits are absolute, and every run is held to them.
#
# A benchmark that breaches a limit fails the run, and the run still goes to the end: the table
# prints, every breach under it with the value it was compared against beside the new one, and
# the recipe fails after that. A build error stops it before anything runs.
[positional-arguments]
bench-code *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    mkdir -p target
    messages=1000
    if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
        messages="$1"
        shift
    fi
    version="$(cargo pkgid gungraun)"
    version="${version##*@}"
    runner="$PWD/target/gungraun-runner"
    installed="$("$runner/bin/gungraun-runner" --version 2> /dev/null || true)"
    if [ "$installed" != "gungraun-runner $version" ]; then
        cargo install --locked --root "$runner" gungraun-runner --version "=$version"
    fi
    unset GUNGRAUN_RUNNER
    export PATH="$runner/bin:$PATH" RUSTFLAGS="" RUSTSTREAM_BENCH_MESSAGES="$messages" \
        GUNGRAUN_HOME="$PWD/target/gungraun/$messages" KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566
    # A baseline named on the command line or in the environment brings the instruction limit.
    baseline="${GUNGRAUN_BASELINE:-}"
    for arg in "$@"; do
        case "$arg" in --baseline | --baseline=*) baseline="$arg" ;; esac
    done
    limits=()
    if [ -n "$baseline" ]; then
        limits=(--callgrind-limits='ir=2.0%')
    fi
    benches=(-p ruststream-kinesis-bench --bench consume --bench reply --bench batch)
    cargo bench "${benches[@]}" --no-run
    status=0
    cargo bench "${benches[@]}" --no-fail-fast \
        -- --output-format=json "${limits[@]}" "$@" > target/bench-code.json || status=$?
    python3 scripts/bench_results.py --code --messages "$messages" target/bench-code.json \
        docs/benchmarks/results.json
    exit "$status"

fmt:
    cargo fmt --all

build:
    cargo build --workspace --release

security: deny zizmor

# Dependency-graph checks (advisories, licenses, duplicates, sources).
# Needs cargo-deny: cargo install cargo-deny --locked
deny:
    cargo deny check

zizmor:
    uvx zizmor .github/workflows

typo:
    uvx codespell

clean:
    cargo clean

ci: check test typo security
