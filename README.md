<h1 align="center">ruststream-kinesis</h1>

<p align="center">
  <i>The Amazon Kinesis Data Streams broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: a sharded, retained log with leases, checkpoints, and replay.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-kinesis/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-kinesis/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-kinesis"><img src="https://img.shields.io/crates/v/ruststream-kinesis.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-kinesis"><img src="https://img.shields.io/crates/dr/ruststream-kinesis" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-kinesis"><img src="https://img.shields.io/docsrs/ruststream-kinesis" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.94.1-blue.svg" alt="MSRV 1.94.1">
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

- **Lazy startup contract.** `KinesisBroker::new()` is synchronous and does no I/O (region and credentials resolve from the environment on connect; `from_config`, or `endpoint` + `region` + `test_credentials`, for local stacks); the runtime connects once at startup, so the broker composes with `#[ruststream::app]`.
- **Checkpoint as acknowledgement.** `ack` marks a record handled; the per-shard watermark advances - and persists - once every earlier record is handled too, because a checkpoint implies everything before it. An unacknowledged record wedges the watermark, so the shard replays from it when the lease is next taken (at-least-once delivery). `nack(requeue = false)` skips (checkpoints past) a poison record.
- **Shard lifecycle owned by the crate.** A coordinator discovers shards (splits and merges included), runs one reader per owned shard, and starts children only after their parents are fully consumed, which preserves per-key ordering across resharding.
- **Pluggable leasing.** The built-in in-process lease store is correct for a single service instance; `DynamoLeaseStore` (feature `dynamodb-lease`) lets multiple instances share the shards with conditional-write fencing - a failed renewal stops the reader immediately.
- **Batch size named by the mount site, spent on the wire.** `.batch(nonzero!(500))` is the one subscription parameter the framework carries down to a broker, and this crate spends it on the wire: it becomes the `GetRecords` limit every shard reader asks with, so a read never fetches more than one batch's worth. Records still reach the runtime one at a time - acknowledgement is a per-shard checkpoint, so it has to be - and a batch closes on the size named or, when the owned shards had less to give, shortly after its first record. It never carries more records than it named. The framework's word comes first and this crate's settings chain after it: `b.include(digest.batch(nonzero!(500)).poll_interval(..))`.
- **Read settings on the descriptor or the mount site.** `poll_interval` prices a read; the 1-second default is the service's own recommendation for staying inside the per-shard budget. `create_if_missing(n)` provisions the stream on subscribe, for local development. Both read the same on `KinesisStream::new("orders")` and chained onto a mount site.
- **One start vocabulary.** Where a subscription reads from is always a `KinesisPosition`; the descriptor carries no separate start options. By default a shard resumes from its stored checkpoint and opens at the tip when it has none. `start_at(KinesisPosition::horizon())` on the subscriber opens it somewhere explicit, and the same positions reposition a running subscription through the delivery context: `Ctx(seeker): Ctx<SeekHandle>` hands a handler the subscription's seeker, `Ctx<Position>` the record's own position. `horizon()`, `latest()` and `timestamp(ms)` are stream-wide, so they reach shards discovered later too; a position captured from a delivered record is shard-scoped and pinned, and seeking to it redelivers exactly that record. Repositioning drops the affected shards' watermark bookkeeping, so a checkpoint from before the seek cannot drag the cursor back.
- **Partition keys as the partition key.** `publisher.with_partition_key("tenant-acme").message(&order).to("orders").publish()` names the record's partition key in front of the framework's publish builder, and `b.include(confirm).out(Reply, Publish::default()).partition_key("receipts-v1")` names it at the mount site, for the publishers a service never holds: a handler's reply and its injected slots. A record that names the key itself wins over both. The `partition-key` header remains the wire, and rides the record's own partition key in both directions (feeding `Partitioned`). The sequence number and shard id are surfaced as headers. User headers beyond that travel in a small conditional envelope - Kinesis records carry only a data blob and a partition key - and plain payloads stay unenveloped.
- **In-process test broker** (feature `testing`). `KinesisTestBroker` routes over a retained log with no server, so `start_at(..)` and a handler's seek handle really re-read it and a service that repositions is unit-testable on the `TestApp` harness. It implements `ruststream::testing::TestableBroker` and passes the framework's routing, `Seekable` and batch conformance suites in process.

Out of scope for this release: enhanced fan-out (a different resume machine on an HTTP/2 push stream, with no local emulator support) and KPL-aggregated records (rejected with an error rather than delivered as opaque protobuf). Kinesis has neither transactions nor request/reply, so this crate ships no policy for either: `Publish` is the whole publish vocabulary.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
# Enables ruststream/testing along with it, so `TestApp` comes with the in-process broker.
ruststream-kinesis = { version = "0.7", features = ["testing"] }
```

`dynamodb-lease` is the other feature: it adds `DynamoLeaseStore` and pulls in `aws-sdk-dynamodb`. A service that hands the store a config it built itself needs `aws-config` as a direct dependency too.

## Write a service

One glob, and it belongs to the file that mounts: `ruststream_kinesis::prelude` re-exports the framework's own prelude alongside this crate's broker, its stream descriptor, its positions and context keys, and its publish policy under the uniform name `Publish`, so a mount site reads the same on every broker. The prefixed original (`KinesisPublish`) stays at the crate root, for a file that mounts two brokers and has to say which `Publish` it means. A handler body imports `ruststream::prelude::*` instead, bounds an injected publisher with a framework capability, and never learns which broker it runs on.

```rust
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize)]
struct Receipt {
    order: u64,
}

// Drop the start_at clause to resume from the checkpoint instead (and open at the tip
// when there is none).
#[subscriber(
    KinesisStream::new("orders"),
    publish("receipts"),
    start_at(KinesisPosition::horizon())
)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        // One key means one shard: every receipt stays ordered against every other, at the
        // cost of a single shard's throughput. Leave it off to spread them instead.
        b.include(confirm)
            .out(Reply, Publish::default())
            .partition_key("receipts-v1");
    })
}
```

The handler names what it replies with; where that reply goes, and under which partition key, is the mount's. Full compiling examples: `crates/ruststream-kinesis/examples/kinesis_service.rs` and `kinesis_replies.rs`.

Multiple instances share the shards through DynamoDB:

```rust
use std::sync::Arc;

use aws_config::BehaviorVersion;
use ruststream_kinesis::prelude::*;

let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
let broker = KinesisBroker::from_config(config.clone())
    .lease_store(Arc::new(DynamoLeaseStore::new(&config, "orders-leases")))
    // Identifies this instance in the lease table; a process-unique value is used when
    // left out.
    .owner_id("instance-a");
```

Full compiling example: `crates/ruststream-kinesis/examples/kinesis_leases.rs`.

## Test it

The `testing` feature runs your real handlers against an in-process Kinesis stand-in on the framework's `TestApp` harness - no server, no docker. It keeps a retained log per stream, so `start_at(..)` and a handler's `SeekHandle` really re-read it, and deliveries carry the same context as a record from the service: a service that seeks mounts unchanged.

```rust
use ruststream::testing::TestApp;
use ruststream_kinesis::prelude::*;
use ruststream_kinesis::testing::KinesisTestBroker;

let app = RustStream::new(AppInfo::new("jobs", "0.1.0"))
    .with_broker(KinesisTestBroker::new(), |b| b.include(work));
let tb = TestApp::start(app).await?;

tb.broker::<KinesisTestBroker>()
    .message(&Job { id: 1 })
    .to("jobs")
    .publish()
    .await?;

tb.broker::<KinesisTestBroker>()
    .subscriber("jobs")
    .assert_called_once()
    .settled(HandlerOutcome::ack());
```

Full compiling examples, including a handler that repositions its own subscription mid-run: `crates/ruststream-kinesis/tests/harness_kinesis.rs`.

The stand-in passes the framework's own `Seekable` and batch conformance suites in process, so the emulation cannot decay into a handle that accepts every seek or a batch longer than the size a mount site named. What a server owns - shard leases, checkpoint durability, replay of unacknowledged records, resharding - is covered by the env-gated live suite instead: `just test-brokers` starts LocalStack and runs the wire and lease checks plus the framework's conformance lifecycle, `Seekable` and batch suites against it.

## Layout

```
ruststream-kinesis/
├── crates/
│   └── ruststream-kinesis/     the published crate
│       ├── examples/           runnable kinesis_* examples
│       └── tests/              TestApp harness, live integration, conformance
├── docs/                       the documentation site
├── docker-compose.test.yml     LocalStack for the live suite
└── Cargo.toml                  workspace
```

## Contributing

```bash
just check          # fmt, clippy, feature checks
just test           # handler-stub tests, no server
just test-brokers   # live integration + conformance against LocalStack
just ci             # check, test, codespell, cargo-deny, zizmor
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
