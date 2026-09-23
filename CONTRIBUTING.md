# Contributing to ruststream-kinesis

`ruststream-kinesis` is the Amazon Kinesis Data Streams broker crate of RustStream. The framework's core crate lives in
[`powersemmi/ruststream`](https://github.com/powersemmi/ruststream), and each of the other broker
crates in a repository of its own. This page covers the environment, the checks a change passes
before review, and how a change is tested against a local checkout of the core.

## Repositories

This crate depends on the core through crates.io and builds on its own. To work on both, clone
them side by side in one directory:

```text
RustStream/
  ruststream/
  ruststream-kinesis/
  ...           the other broker crates, when a change reaches them too
```

```bash
mkdir RustStream && cd RustStream
git clone https://github.com/powersemmi/ruststream.git
git clone https://github.com/powersemmi/ruststream-kinesis.git
```

## Environment

- **Rust** through rustup. `rust-toolchain.toml` selects stable with rustfmt and clippy. The
  minimum supported version is 1.94.1, the `rust-version` in `Cargo.toml`:
  `rustup toolchain install 1.94.1` builds against it with
  `cargo +1.94.1 check --workspace --all-features`.
- **just**, which runs every recipe below.
- **Docker** with Compose, for the LocalStack stand (Kinesis and DynamoDB) the live suite and the benchmarks run against.
- Per task:

| Task | Tool | Install |
| --- | --- | --- |
| `just deny` | cargo-deny | `cargo install cargo-deny --locked` |
| `just typo`, `just zizmor` | uv | the uv documentation |
| `just bench` | Python 3 | the system package manager |
| the documentation site | Python 3.12 | `pip install -r docs/requirements.txt`, then `properdocs serve` |

## Checking a change

```bash
just check          # rustfmt, clippy, cargo check with all features and with none
just test           # the in-process suite
just test-brokers   # the live suite against LocalStack
just ci             # check and test, plus codespell, cargo deny and zizmor
```

`just test` runs the suite in process; the live tests skip themselves there. `just test-brokers`
starts the Compose stand, runs the whole suite against LocalStack with the live tests required, and
stops the stand.

`just bench` measures what this crate and the framework's runtime cost over the raw
`aws-sdk-kinesis` client on the same stand and rewrites `docs/benchmarks/results.json`. It takes
minutes and wants the machine to itself.

## Testing against a local core

A change in the core is tested here before it is released, with the `ruststream` dependency
patched to the checkout next to this one:

```bash
cargo test --workspace --all-features --config "patch.crates-io.ruststream.path='../ruststream'"
```

From the core, `just brokers kinesis` runs the same command against the core's working tree.

For the live suite against the local core, start the stand, run the command `test-brokers` runs
with the core patched in, and stop the stand:

```bash
just brokers-up
KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 RUSTSTREAM_REQUIRE_LIVE=1 \
    cargo test --workspace --all-features \
    --config "patch.crates-io.ruststream.path='../ruststream'" -- --test-threads=1
just brokers-down
```

## Pull requests

- One logical change per pull request.
- Open it as a draft. CI runs when it is marked ready for review.
- A pull request merges as one squashed commit, after the `CI result` check and one approving
  review. Commits are signed.
- Documentation changes with the code. An item's rustdoc says what it is and does, and the crate's
  module overviews are its guides. The site holds the entry pages in English, Russian and Chinese,
  and an edit reaches all three.
