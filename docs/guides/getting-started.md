---
sidebar_position: 1
description: "Install ruststream-kinesis, start a local Kinesis, run a first subscriber against it and test it without a server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-kinesis`, starts a local Kinesis and runs a first service against it.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-kinesis = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the default codec, and `logging` prints the service's log to the terminal.
`ruststream-kinesis` is the Kinesis broker, built on the official `aws-sdk-kinesis` client.
Its `testing` feature adds the [in-process Kinesis](api:ruststream::testing::TestApp) that tests run on.

The broker crate has these features, all off by default:

| Feature | What it gives |
|---|---|
| `dynamodb-lease` | [`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md), so several instances of a service share the shards ([Several instances](leases.md)) |
| `asyncapi` | the Kinesis details of each channel in the generated [AsyncAPI](api:ruststream::asyncapi) document |
| `testing` | the in-process Kinesis behind `TestApp` |

## A local Kinesis

[LocalStack](https://github.com/localstack/localstack) runs Kinesis in one container:

```yaml title="compose.yaml"
services:
  localstack:
    image: localstack/localstack:4.14.0
    ports:
      - "4566:4566"
    environment:
      SERVICES: kinesis
      KINESIS_LATENCY: 0
```

```bash
docker compose up -d
```

The stack answers on `http://localhost:4566` and starts with no streams.
`KINESIS_LATENCY: 0` turns off the delay LocalStack adds to every Kinesis call by default.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` reads JSON orders from the `orders` stream.
It acknowledges an order with an amount and drops one without.
[`KinesisStream`](../components/subscriptions/kinesis-stream.md) names the stream, and [`create_if_missing(1)`](../components/subscriptions/create-if-missing.md) creates it with one shard when the subscription opens.
A production service leaves that step out and takes its streams from its infrastructure.
[`KinesisBroker::new`](api:ruststream_kinesis::KinesisBroker::new) only records the configuration.
[`endpoint`](api:ruststream_kinesis::KinesisBroker::endpoint), [`region`](api:ruststream_kinesis::KinesisBroker::region) and [`test_credentials`](api:ruststream_kinesis::KinesisBroker::test_credentials) point it at the local stack.
The SDK client is built when the service starts.
`#[ruststream::app]` turns `app` into `main`; in the manual form, [`run_main`](api:ruststream::runtime::cli::run_main) does the same.

Run the service:

```bash
cargo run -- run
```

The subscription opens at the tip of the stream, so it reads what is published after the service started.
Publish an order from another terminal with the AWS CLI:

```bash
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
aws kinesis put-record --stream-name orders --partition-key customer-7 \
  --cli-binary-format raw-in-base64-out --data '{"id":1,"amount_cents":1250}'
```

The service logs `order taken` with the order's id.
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops it.

## The first test

The example file also holds two tests.
Each starts the same `app` with `TestApp::start`, publishes an order and checks how `take_order` settled it.
`KinesisBroker` connects to an in-process Kinesis there, so the tests need no stack:

```bash
cargo test
```

[Testing a Kinesis service](testing.md) walks through these tests and what the in-process Kinesis models.

## Coming from aws-sdk-kinesis

The Rust SDK reads a stream one shard and one call at a time, and a service on it writes the consumer around those calls.
Here is where each part goes in RustStream:

| aws-sdk-kinesis | RustStream |
|---|---|
| `aws_config::load_from_env` and `Client::new` | [`KinesisBroker::new`](api:ruststream_kinesis::KinesisBroker::new), or [`from_config`](api:ruststream_kinesis::KinesisBroker::from_config) with an `SdkConfig` of your own ([Running in production](production.md)) |
| `ListShards` and a `GetShardIterator` per shard | the stream name in [`KinesisStream::new`](../components/subscriptions/kinesis-stream.md); the subscription finds the [shards](../components/subscriptions/shards.md) and follows splits and merges |
| a `GetRecords` loop per shard | a `#[subscriber]` handler ([Consuming a stream](consuming.md)) |
| decoding `record.data()` | the handler's parameter type, decoded by the codec |
| a checkpoint written after the work | the handler's outcome ([Checkpoints and delivery guarantees](checkpoints.md)) |
| a lease table so that several instances share the shards | [`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md) ([Several instances](leases.md)) |
| `GetShardIterator` with `TRIM_HORIZON` or `AT_TIMESTAMP` | [`start_at`](api:ruststream::runtime::SubscriberSettings::start_at) at the mount site, or the seek handle in the handler ([Replay and seeking](replay.md)) |
| `PutRecord` with a partition key | a reply or an `Out` parameter, sent through [`KinesisPublish`](api:ruststream_kinesis::KinesisPublish) ([Shards, partition keys and ordering](ordering.md)) |
