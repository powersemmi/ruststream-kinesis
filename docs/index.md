# ruststream-kinesis

**`ruststream-kinesis`** is the Amazon Kinesis Data Streams broker for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework, built on the official
[`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis). On top of the SDK it supplies the
coordination the vendor's consumer library provides on other platforms and the Rust SDK does not:
shard discovery across splits and merges, shard leasing with fencing, and per-shard checkpointing.

Handlers, routers, codecs, and middleware come from the framework; this crate supplies the
transport, and nothing broker-specific leaks back into the framework.

```toml
ruststream = { version = "0.6", features = ["macros", "json"] }
ruststream-kinesis = "0.6"
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

This site documents the Kinesis broker only. Framework concepts that apply to every broker (writing
subscribers, publishing, routing, codecs, middleware, observability, the CLI) live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/). The pages here cover what is
specific to Kinesis and link back to the framework docs where the two meet.
