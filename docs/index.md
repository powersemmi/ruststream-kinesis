# ruststream-kinesis

**`ruststream-kinesis`** runs a [RustStream](https://powersemmi.github.io/ruststream/) service on
Amazon Kinesis Data Streams. A stream is a sharded log, like Kafka.

The transport is implemented over the official [`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis)
client. On top of it, a subscription discovers the stream's shards across splits and merges, leases
each shard with fencing, and checkpoints each shard's progress.

The `testing` feature ships an in-process broker.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_service.rs:app"
```

## Where to go next

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Kinesis guide](kinesis.md)** - subscriptions, leases and checkpoints, positions, publishing, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-kinesis)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site documents the Kinesis broker only. Everything that works the same on every broker is in
the [RustStream documentation](https://powersemmi.github.io/ruststream/).
