---
title: Testing
description: "TestApp::start connects KinesisBroker in process: one retained log per stream, positions and seeking, and the service's refusals."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`KinesisBroker`](../broker/index.md) in process: one retained log per stream, positions and seeking, and the service's refusals.

A test hands the harness the app `main` runs, unchanged, and addresses the broker by its production type, `tb.broker::<KinesisBroker>()`.
The `testing` feature in `[dev-dependencies]` adds the in-process mode, and turns on the core's harness with it.
The in-process transport has no settings of its own.
The descriptors and the publish policies of the app are the production ones, and the AWS configuration is never read.

A record is written the way the publisher writes it for the service: the [partition key](../publishing/keys.md) resolved in the same order, the headers in the same [envelope](../context/envelope.md).
It is read back by the same decoding, so a delivery carries the [position](../positions/index.md), the [three headers](../context/headers.md) and the context a record from a shard carries.
Each stream is one retained log with one shard, so every record of a stream is ordered against every other.
A subscription opens at the tip, and [`start_at`](../positions/start-at.md) and a [seek](../positions/seek.md) read the retained log again from the position they name.
A plain [`retry`](../settlement/index.md) is a requeue the stream cannot perform, so the record does not come back; a [`retry_after`](../settlement/retry-after.md) copy does, once `tb.advance()` passes its delay under `tokio::time::pause()`.

The transport refuses what the service refuses.
A stream name the service does not accept, a partition key outside 1 to 256 characters, and a record over 10 MiB, the most a stream can be configured to take, are refused with the service's error.
A record a handler publishes there gets the error back, as it would from the service.
A record the test itself publishes there fails the test at the publish.
A publisher or a seek handle used after shutdown reports [`KinesisError::NotConnected`](../broker/shutdown.md).

A publish through the harness returns once every handler it woke has settled, the records those handlers published included.
The assertions that follow read finished state.
What only the service has is for a [live test](./live.md): a stream's shards and the hashing of keys to them, the existence of a stream, leases and checkpoints, resharding, retention, throttling, credentials and endpoints.

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-kinesis = { version = "0.7", features = ["testing"] }
```

The first test publishes a payment and reads how the handler settled it and what it booked on the ledger.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_testing.rs#testapp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_testing.rs#testapp"
```

</TabItem>
</Tabs>

A payment whose account is too long to be a partition key cannot be booked: the transport refuses the entry, as the service would, and the handler settles the payment as a retry.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_testing.rs#refusals"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_testing.rs#refusals"
```

</TabItem>
</Tabs>
