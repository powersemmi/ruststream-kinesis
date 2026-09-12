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
- **Batches the service builds.** A batch handler names its size at the mount site, and that size is the `GetRecords` limit every shard reader asks with: `b.include(digest.batch(nonzero!(500)).poll_interval(...))`. A read never fetches more than one batch's worth, and no batch carries more records than it named. The framework's word comes first and this crate's settings chain after it.
- **Explicit polling settings.** `KinesisStream::new("orders").poll_interval(...)` - polling stays within the service's per-shard budget by default.
- **One start vocabulary.** Where a subscription reads from is always a `KinesisPosition`; the descriptor carries no separate start options. By default a shard resumes from its stored checkpoint and opens at the tip when it has none. `start_at(KinesisPosition::horizon())` on the subscriber opens it somewhere explicit, and the same positions reposition a running subscription through the delivery context: `Ctx(seeker): Ctx<SeekHandle>` hands a handler the subscription's seeker, `Ctx<Position>` the record's own position. `horizon()`, `latest()` and `timestamp(ms)` are stream-wide, so they reach shards discovered later too; a position captured from a delivered record is shard-scoped and pinned, and seeking to it redelivers exactly that record. Repositioning drops the affected shards' watermark bookkeeping, so a checkpoint from before the seek cannot drag the cursor back.
- **Partition keys as the partition key.** `publisher.with_partition_key("tenant-acme").message(&order).publish()` names the record's partition key in front of the framework's publish builder, and `b.include(confirm).out(Reply, Publish::default()).partition_key("receipts-v1")` names it at the mount site, for the publishers a service never holds: a handler's reply and its injected slots. A record that names the key itself wins over both. The `partition-key` header remains the wire, and rides the record's own partition key in both directions (feeding `Partitioned`). The sequence number and shard id are surfaced as headers. User headers beyond that travel in a small conditional envelope - Kinesis records carry only a data blob and a partition key - and plain payloads stay unenveloped.
- **In-process test broker** (feature `testing`). `KinesisTestBroker` routes over a retained log with no server, so `start_at(..)` and a handler's seek handle really re-read it and a service that repositions is unit-testable on the `TestApp` harness. The descriptor and the publish policy a service ships mount on it unchanged - there is no test-only spelling of either - and it implements `ruststream::testing::TestableBroker` and answers every contract suite the framework ships for this broker: routing, the lifecycle ladder, `Seekable` and batches, all in process as well as against LocalStack.

Out of scope for this release: enhanced fan-out (a different resume machine on an HTTP/2 push stream, with no local emulator support) and KPL-aggregated records (rejected with an error rather than delivered as opaque protobuf).

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
```

## Write a service

One glob: `ruststream_kinesis::prelude` carries the framework's own prelude alongside this crate's
broker, descriptors, positions, and publish policy.

```rust
use ruststream_kinesis::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// Drop the start_at clause to resume from the checkpoint instead (and start at the tip
// when there is none).
#[subscriber(KinesisStream::new("orders"), start_at(KinesisPosition::horizon()))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
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
use ruststream_kinesis::prelude::*;

# async fn wire(config: aws_config::SdkConfig) {
let broker = KinesisBroker::from_config(config.clone())
    .lease_store(Arc::new(DynamoLeaseStore::new(&config, "orders-leases")));
# let _ = broker;
# }
```

## Test it

The `testing` feature runs your real handlers against an in-process Kinesis stand-in on the framework's `TestApp` harness - no server, no docker. It keeps a retained log per stream, so `start_at(..)` and a handler's `SeekHandle` really re-read it, and deliveries carry the same context as a record from the service: a service that seeks mounts unchanged.

The mount is the one you ship: `KinesisStream` opens a subscription on the stand-in - the settings it carries included - and `Publish` pairs with it, so a routes file changes its broker and nothing else.

```rust
use ruststream::testing::TestApp;
use ruststream_kinesis::prelude::*;
use ruststream_kinesis::testing::KinesisTestBroker;

let app = RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(KinesisTestBroker::new(), |b| {
    b.include(work);
    b.include(confirm).out(Reply, Publish::default().partition_key("tenant-acme"));
});
let tb = TestApp::start(app).await?;

tb.broker::<KinesisTestBroker>().message(&Job { id: 1 }).to("jobs").publish().await?;
tb.broker::<KinesisTestBroker>()
    .subscriber("jobs")
    .assert_called_once()
    .settled(HandlerOutcome::ack());
```

The framework's contract suites run against the stand-in, not only against the server: routing, the lifecycle ladder (down to a publisher created before `shutdown` erroring afterwards), `Seekable`, and batches. The emulation therefore cannot decay into a handle that accepts every seek, a batch longer than the size a mount site named, or a transport that keeps accepting records after it closed. The same suites run live, which is what proves the contract is the product's: `just test-brokers` starts LocalStack and runs the wire and lease checks plus the framework's lifecycle, `Seekable` and batch suites against it. What only a server owns - shard leases, checkpoint durability, replay of unacknowledged records, resharding - stays in that live suite.

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
