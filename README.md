<h1 align="center">ruststream-kinesis</h1>

<p align="center">
  <i>The Amazon Kinesis Data Streams broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: a sharded, retained log with leases, checkpoints, and replay.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-kinesis/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-kinesis/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/MSRV-1.94-blue.svg" alt="MSRV 1.94">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

---

`ruststream-kinesis` implements the RustStream broker contract over the official [`aws-sdk-kinesis`](https://crates.io/crates/aws-sdk-kinesis) - plus the coordination the vendor's consumer library provides on other platforms and the Rust SDK does not: shard discovery across splits and merges, shard leasing with fencing, and per-shard checkpointing. Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport.

## Features

- **Lazy startup contract.** `KinesisBroker::new()` is synchronous and does no I/O (environment resolution on connect; `from_config`, `endpoint` + `test_credentials` for local stacks); the runtime connects once at startup, so the broker composes with `#[ruststream::app]`.
- **Checkpoint as acknowledgement.** `ack` marks a record handled; the per-shard watermark advances - and persists - once every earlier record is handled too, because a checkpoint implies everything before it. An unacknowledged record wedges the watermark, so the shard replays from it when the lease is next taken: at-least-once, always. `nack(requeue = false)` skips (checkpoints past) a poison record.
- **Shard lifecycle owned by the crate.** A coordinator discovers shards (splits and merges included), runs one reader per owned shard, and starts children only after their parents are fully consumed - which is what keeps per-key ordering across resharding.
- **Pluggable leasing.** The built-in in-process lease store is correct for a single service instance; `DynamoLeaseStore` (feature `dynamodb-lease`) lets multiple instances share the shards with conditional-write fencing - a failed renewal stops the reader immediately.
- **Explicit consumer economics.** `KinesisStream::new("orders").batch(1000).poll_interval(...)` - polling stays within the service's per-shard budget by default.
- **One start vocabulary.** Where a subscription reads from is `KinesisPosition` and nothing else; the descriptor carries no parallel start enum. By default a shard resumes from its stored checkpoint and opens at the tip when it has none. `start_at(KinesisPosition::horizon())` on the subscriber opens it somewhere explicit, and the same positions reposition a running subscription through `Seek(seeker): Seek<KinesisSeeker>`. `horizon()`, `latest()` and `timestamp(ms)` are stream-wide, so they reach shards discovered later too; a position captured from a delivered record is shard-scoped and pinned, and seeking to it redelivers exactly that record. Repositioning drops the affected shards' watermark bookkeeping, so a checkpoint from before the seek cannot drag the cursor back.
- **Partition keys as the partition key.** The `partition-key` header rides the record's own partition key in both directions (feeding `Partitioned`); the sequence number and shard id are surfaced as headers. User headers beyond that travel in a small conditional envelope - Kinesis records carry only a data blob and a partition key - and plain payloads stay unenveloped.
- **In-process test broker** (feature `testing`). `KinesisTestBroker` reproduces core routing with no server, implements `ruststream::testing::TestableBroker`, and passes the framework's conformance suite in process.

Deliberately scoped out of this release: enhanced fan-out (a different resume machine on an HTTP/2 push stream, with no local emulator support) and KPL-aggregated records (refused loudly rather than delivered as opaque protobuf).

## Status

Implemented and verified against LocalStack (the framework's conformance lifecycle suite and the integration tests - including checkpoint resume and unacknowledged-record replay - run in CI against it). Built on the `ruststream` 0.6 line; this crate is not published to crates.io yet. Design and scope are tracked in [powersemmi/ruststream#194](https://github.com/powersemmi/ruststream/issues/194).

MSRV is 1.94, tracking the AWS SDK (the core stays at 1.85; a dependent may exceed its dependency's floor).

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

The `testing` feature runs handlers against an in-process Kinesis stand-in - no server, same routing. Product behaviour (shard leases, checkpoint resume, replay of unacknowledged records) is covered by the env-gated live suite instead: `just test-brokers` starts LocalStack and runs the integration tests plus the framework conformance lifecycle against it.

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
