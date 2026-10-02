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

`ruststream-kinesis` connects a RustStream service to Amazon Kinesis Data Streams over the
official [`aws-sdk-kinesis`](https://crates.io/crates/aws-sdk-kinesis). It also does the
coordination the vendor's consumer library does on other platforms: shard discovery across splits
and merges, shard leases, and per-shard checkpoints. Handlers, routing, codecs and middleware come
from the framework; this crate is the transport.

## Features

- **Checkpoints as acknowledgement:** a shard's checkpoint advances once every earlier record is
  handled, so delivery is at least once.
- **Shards handled by the crate:** one reader per owned shard, and children start after their
  parents, so per-key order survives resharding.
- **Leases per stream and shard:** a lease and its checkpoint belong to a stream and a shard
  together, so one lease store serves every stream a broker consumes. Instances share them
  through DynamoDB behind the `dynamodb-lease` feature.
- **Batches** sized at the mount site and read with one `GetRecords` call.
- **Start positions and repositioning:** the horizon, the tip, a timestamp or a captured record.
- **The partition key** as a per-message setting.
- **Deferred retries,** with retry caps and dead-letter streams.
- **AsyncAPI** with the stream, read pause and shards, behind the `asyncapi` feature.
- **Tests on the production app** (feature `testing`): `TestApp` runs it with `KinesisBroker`
  connected in process, with no AWS account, and `TestApp::start_live` runs the same test against
  LocalStack.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-kinesis = { version = "0.7", features = ["testing"] }
```

## Write a service

```rust
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct Receipt {
    order: u64,
}

#[subscriber(
    KinesisStream::new("orders"),
    reply("receipts"),
    start_at(KinesisPosition::horizon())
)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(confirm)
            .out_reply(Publish::default())
            .partition_key("receipts-v1");
    })
}
```

`#[ruststream::app]` generates `main`, so the binary understands `run` and `asyncapi gen`.

## Upgrading a DynamoDB lease table

The table keeps one row per stream and shard, keyed `stream:shard` in the `lease_key` attribute. Versions before this one keyed a row by the shard id alone, so a table they wrote holds rows that name no stream. After the upgrade:

1. If the table served one stream, name it: `DynamoLeaseStore::new(&config, "orders-leases").legacy_stream("orders")`. A shard of that stream with no row of its own resumes from its bare row, and the first read copies that checkpoint under `orders:shardId-...`. Other streams ignore the bare rows.
2. Without that setting, a shard whose bare row holds a checkpoint is not read: the subscription reports a lease error naming the table, the shard and the stream, so no stream starts from another stream's progress or from the tip by accident.
3. Once the upgraded service has started every stream it consumes, each bare row that held progress has its copy. Delete the rows whose `lease_key` has no `:` and drop `legacy_stream`.

A table that served more than one stream before the upgrade holds, in each bare row, whichever stream checkpointed last, so neither stream can resume from it: delete the bare rows and open the streams with `start_at(..)` at the position they should read from. Stop the instances of the old version before starting the new one: the two versions lease different rows, so running both at once lets both read the same shard (duplicates, not losses).

## Test it

`TestApp::start` runs the service's own app with `KinesisBroker` connected in process, with no AWS
account. `TestApp::start_live(app())` runs the same test against LocalStack.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(app()).await?;

tb.broker::<KinesisBroker>()
    .message(&Order { id: 42 })
    .to("orders")
    .publish()
    .await?;

tb.broker::<KinesisBroker>()
    .subscriber("orders")
    .assert_called_once()
    .with(&Order { id: 42 })
    .settled(HandlerOutcome::ack());

tb.broker::<KinesisBroker>()
    .published::<Receipt>("receipts")
    .assert_called_once()
    .with(&Receipt { order: 42 });
```

## Documentation

- This crate: <https://docs.rs/ruststream-kinesis>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.94.1**, edition 2024, the floor of the AWS SDK.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
