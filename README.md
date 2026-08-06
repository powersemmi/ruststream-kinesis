<h1 align="center">ruststream-kinesis</h1>

<p align="center">
  <i>The Amazon Kinesis Data Streams broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: a sharded, retained log with leases, checkpoints, and replay.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-kinesis/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-kinesis/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-kinesis"><img src="https://img.shields.io/crates/v/ruststream-kinesis.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-kinesis"><img src="https://img.shields.io/crates/dr/ruststream-kinesis" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-kinesis"><img src="https://img.shields.io/docsrs/ruststream-kinesis" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.94-blue.svg" alt="MSRV 1.94">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-kinesis/">Documentation</a></b>
</p>

---

`ruststream-kinesis` implements the RustStream broker contract over the official [`aws-sdk-kinesis`](https://crates.io/crates/aws-sdk-kinesis) - plus the coordination the vendor's consumer library provides on other platforms and the Rust SDK does not: shard discovery across splits and merges, shard leasing with fencing, and per-shard checkpointing. Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport.

## Features

- **Lazy startup contract.** `KinesisBroker::new()` is synchronous and does no I/O (environment resolution on connect; `from_config`, `endpoint` + `test_credentials` for local stacks); the runtime connects once at startup, so the broker composes with `#[ruststream::app]`.
- **Checkpoint as acknowledgement.** `ack` marks a record handled; the per-shard watermark advances - and persists - once every earlier record is handled too, because a checkpoint implies everything before it. An unacknowledged record wedges the watermark, so the shard replays from it when the lease is next taken (at-least-once delivery). `nack(requeue = false)` skips (checkpoints past) a poison record.
- **Shard lifecycle owned by the crate.** A coordinator discovers shards (splits and merges included), runs one reader per owned shard, and starts children only after their parents are fully consumed, which preserves per-key ordering across resharding.
- **Pluggable leasing.** The built-in in-process lease store is correct for a single service instance; `DynamoLeaseStore` (feature `dynamodb-lease`) lets multiple instances share the shards with conditional-write fencing - a failed renewal stops the reader immediately.
- **Explicit polling settings.** `KinesisStream::new("orders").batch(1000).poll_interval(...)` - polling stays within the service's per-shard budget by default.
- **One start vocabulary.** Where a subscription reads from is always a `KinesisPosition`; the descriptor carries no separate start options. By default a shard resumes from its stored checkpoint and opens at the tip when it has none. `start_at(KinesisPosition::horizon())` on the subscriber opens it somewhere explicit, and the same positions reposition a running subscription through `Seek(seeker): Seek<KinesisSeeker>`. `horizon()`, `latest()` and `timestamp(ms)` are stream-wide, so they reach shards discovered later too; a position captured from a delivered record is shard-scoped and pinned, and seeking to it redelivers exactly that record. Repositioning drops the affected shards' watermark bookkeeping, so a checkpoint from before the seek cannot drag the cursor back.
- **Partition keys as the partition key.** The `partition-key` header rides the record's own partition key in both directions (feeding `Partitioned`); the sequence number and shard id are surfaced as headers. User headers beyond that travel in a small conditional envelope - Kinesis records carry only a data blob and a partition key - and plain payloads stay unenveloped.
- **In-process test broker** (feature `testing`). `KinesisTestBroker` reproduces core routing with no server, implements `ruststream::testing::TestableBroker`, and passes the framework's conformance suite in process.

Out of scope for this release: enhanced fan-out (a different resume machine on an HTTP/2 push stream, with no local emulator support) and KPL-aggregated records (rejected with an error rather than delivered as opaque protobuf).

## Install

```toml
[dependencies]
ruststream = { version = "0.6", features = ["macros", "json"] }
ruststream-kinesis = "0.6"
serde = { version = "1", features = ["derive"] }
```

## Write a service

```rust
use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_kinesis::{KinesisBroker, KinesisPosition, KinesisStream};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// Drop the start_at clause to resume from the checkpoint instead (and start at the tip
// when there is none).
#[subscriber(KinesisStream::new("orders"), start_at(KinesisPosition::horizon()))]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(KinesisBroker::new(), |b| b.include(handle))
}
```

Multiple instances share the shards through DynamoDB:

```rust
use std::sync::Arc;
use ruststream_kinesis::{DynamoLeaseStore, KinesisBroker};

# async fn wire(config: aws_config::SdkConfig) {
let broker = KinesisBroker::from_config(config.clone())
    .lease_store(Arc::new(DynamoLeaseStore::new(&config, "orders-leases")));
# let _ = broker;
# }
```

## Test it

The `testing` feature runs handlers against an in-process Kinesis stand-in - no server, same routing, same ladder. Inject a record as an external producer would with `TestableBroker::inject`, then assert on what a handler published with the free `expect_published`:

```rust
use ruststream::{Broker, OutgoingMessage};
use ruststream::testing::{TestableBroker, expect_published};
use ruststream_kinesis::testing::KinesisTestBroker;

let broker = KinesisTestBroker::new().connect().await?;
broker.inject(OutgoingMessage::new("orders", br#"{"id":1}"#));
let confirmations =
    expect_published(&broker, "confirmations", 1, std::time::Duration::from_secs(1)).await;
```

Kinesis behaviour (shard leases, checkpoint resume, replay of unacknowledged records) is covered by the env-gated live suite instead: `just test-brokers` starts LocalStack and runs the integration tests plus the framework conformance lifecycle against it.

## Layout

```
ruststream-kinesis/
├── crates/
│   └── ruststream-kinesis/     the published crate
│       └── examples/           runnable kinesis_* examples
├── docker-compose.test.yml     LocalStack for the live suite
└── Cargo.toml                  workspace
```

## Contributing

```bash
just check          # fmt, clippy, feature checks
just test           # handler-stub tests, no server
just test-brokers   # live integration + conformance against LocalStack
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
