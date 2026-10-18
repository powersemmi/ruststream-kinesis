---
title: Subscriptions
description: "A stream name in #[subscriber] reads every shard of that stream; KinesisStream carries every other setting of the subscription."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

A stream name in `#[subscriber]` reads every shard of that stream; [`KinesisStream`](./kinesis-stream.md) carries every other setting of the subscription.

`#[subscriber("orders")]` reads the stream `orders` with the descriptor's defaults: [`KinesisStream::new("orders")`](api:ruststream_kinesis::KinesisStream::new).
A stream is named by its name, which the service limits to 128 characters of letters, digits, `_`, `.` and `-`.
The subscription finds the stream's [shards](./shards.md), takes a [lease](../leases/index.md) on each one and reads each leased shard with its own reader.
A shard resumes from its stored checkpoint, and a shard without one opens at the tip; [`start_at`](../positions/start-at.md) names another position.
The subscription is open once every shard it took has a reader and a starting cursor, so a record published after startup reaches it.
A stream the service does not have stops the service at startup, unless the descriptor [creates it](./create-if-missing.md).
Delivery is at-least-once, and an acknowledgement moves the shard's checkpoint, as [Checkpoint](../settlement/index.md) describes.
The manual form passes the name to [`subscriber(name, body)`](api:ruststream::runtime::subscriber).

## Example

The stream `orders`, read by one handler.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_subscriptions_name.rs#name"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_subscriptions_name.rs#name"
```

</TabItem>
</Tabs>
