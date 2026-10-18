---
sidebar_position: 3
title: Startup
description: "A subscription opens once every shard it took has a reader and a starting cursor; a stream the service lacks fails startup unless the descriptor creates it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Startup

A subscription opens once every shard it took has a reader and a starting cursor; a stream the service lacks fails startup unless the descriptor [creates it](../subscriptions/create-if-missing.md).

Each subscription opens in three steps.
First the descriptor is checked before any request: an empty stream name or a shard count below one fails startup with [`KinesisError::Invalid`](api:ruststream_kinesis::KinesisError::Invalid).
Then one `DescribeStreamSummary` asks the service for the stream.
A stream it does not have fails startup with [`KinesisError::Stream`](api:ruststream_kinesis::KinesisError::Stream), which names the stream, so a mistyped name stops the service before it reads anything.
Last, the subscription lists the stream's [shards](../subscriptions/shards.md), takes a [lease](../leases/index.md) on each shard it can, and opens a reader with its starting cursor on each one.

The subscription is open when that first pass is done.
From that moment every record published to the stream reaches it.
A shard another instance holds is left to that instance, and the subscription takes it later if its lease lapses.
A failure inside the first pass, such as a lease store that does not answer, reaches the subscription's stream after startup and is logged there.

One broker reads a stream through one subscription.
A second subscription of the same broker to a stream it already reads is refused when it opens.

## Example

The stream `payouts` is the platform team's, and the service never creates it.
On a stand without that stream, the service stops at startup.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_broker_startup.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_broker_startup.rs#service"
```

</TabItem>
</Tabs>
