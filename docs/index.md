# ruststream-kinesis

**`ruststream-kinesis`** runs a [RustStream](https://powersemmi.github.io/ruststream/) service on
Amazon Kinesis Data Streams. A stream is a sharded, retained log, like Kafka.

The transport is implemented over the official [`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis)
client. On top of it, a subscription discovers the stream's shards across splits and merges, leases
each shard with fencing, and checkpoints each shard's progress. An acknowledgement is a checkpoint,
one service instance reads a shard at a time, and delivery is at-least-once.

## Install

Three features, all off by default: `dynamodb-lease` shares the shards between service instances,
`testing` ships the in-process broker, and `asyncapi` writes this broker's own vocabulary into the
generated document.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
```

## The first service

A handler is an `async fn` over a decoded record, and `KinesisStream` names the stream it reads.
The application object mounts the handler on the broker, and the attribute writes the `main`:

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_service.rs:app"
```

`cargo run -- run` starts it. The broker records configuration only; the region and the credentials
resolve when the runtime connects it.

## What the crate offers

The crate's rustdoc is its guide, written next to the code it describes:

- [Subscribing](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#subscribing)
  - the stream descriptor, the pause between reads, and creating a stream for a local stand.
- [Leases and checkpoints](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#leases-and-checkpoints)
  - what an acknowledgement writes, the `DynamoDB` store several service instances share, and
  upgrading a lease table an earlier version wrote.
- [Positions and seeking](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#positions-and-seeking)
  - opening a subscription anywhere the stream still retains, and repositioning it from a handler.
- [Batches](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#batches)
  - the size a batch mount names becomes the read limit every shard reader asks with.
- [Publishing](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#publishing)
  - the publish policy, and the partition key that picks a record's shard.
- [Testing](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#testing)
  - the in-process transport behind the `testing` feature.

## Where to go next

<div class="grid cards" markdown>

- :material-language-rust: **[API reference](https://docs.rs/ruststream-kinesis)** - the crate's rustdoc on docs.rs, which is also its guide.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - installation, the tutorial and the list of brokers.
- :material-transit-connection-variant: **[Framework reference](https://docs.rs/ruststream)** - subscribers, routing, codecs, middleware, the CLI.

</div>

This site documents the Kinesis broker only. Everything that works the same on every broker is in
the [RustStream documentation](https://powersemmi.github.io/ruststream/).
