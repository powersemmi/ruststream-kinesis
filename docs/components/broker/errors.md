---
sidebar_position: 6
title: Errors
description: "KinesisError is the crate's one error type: stream admin, reads, aggregated records, publishes, leases, a closed connection and invalid descriptors."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`KinesisError`](api:ruststream_kinesis::KinesisError) is the crate's one error type: stream admin, reads, aggregated records, publishes, leases, a closed connection and invalid descriptors.

Each variant names the step that failed, and most also name the stream and the shard.
The SDK error behind it is its `source`, formatted with its whole cause chain, so the SDK's types stay out of the crate's API.

| Variant | When |
|---|---|
| `Stream` | the stream is missing, or describing or creating it failed |
| `Read` | listing the shards, opening an iterator or reading records failed, or a seek named a shard this instance does not read |
| `AggregatedRecord` | a record was written by the Kinesis Producer Library's aggregation, which the crate does not unpack |
| `Publish` | the service refused a record or did not store it |
| `Lease` | the lease store failed, or refused a row it cannot read |
| `NotConnected` | a publisher or a seek handle was used after the broker shut down |
| `Invalid` | the descriptor cannot work, such as an empty stream name or a shard count below one |

A service meets the error in three places.
A failed startup carries it as the source of the runtime's error, and the service exits before it reads anything.
A publish from a handler returns it inside the core's `PublishError`, and the handler decides how the delivery settles.
A failure while a subscription runs, such as a read that keeps failing, is logged by the runtime as an error that names the stream and the shard.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

## Example

An archive record the service refuses is logged with its cause, and the invoice is settled as a retry.
The mount site caps the retries at three deliveries, and then the invoice leaves for `invoices.unarchived`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_broker_errors.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_broker_errors.rs#handler"
```

</TabItem>
</Tabs>
