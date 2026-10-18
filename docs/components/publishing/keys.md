---
sidebar_position: 1
title: Keys
description: "A record's partition key picks its shard and its order; four places can name it, and a record nobody keyed gets a process-unique one."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Keys

A record's partition key picks its shard and its order; four places can name it, and a record nobody keyed gets a process-unique one.

The service hashes the key to pick the shard, so every record of one key lands on one shard, and a reader sees them in the order they were written.
A key is 1 to 256 characters; the service refuses a record with any other with [`KinesisError::Publish`](api:ruststream_kinesis::KinesisError::Publish).
One key is one shard's write throughput, which the service limits per shard, so a key that every record shares orders them all at the cost of that throughput.

The publisher takes the first key it finds, in this order:

1. the [`partition_key`](./partition-key.md) step of the publish;
2. the `partition-key` header, [`PARTITION_KEY_HEADER`](api:ruststream_kinesis::PARTITION_KEY_HEADER), that the message carries;
3. the key the mount site named on the policy, with [`KinesisPublishSettings`](./publish-settings.md);
4. a key unique to the process, which spreads the records over the shards.

The header is the portable spelling: a body that names the key that way stays free of this crate, and a message that crossed from another broker of the framework carries its key in it.
A message type declares the header in its typed header contract, `#[outgoing(headers = ..)]`, with a field renamed to `partition-key`; a `HeaderMap` with that entry works the same.
The key travels in the record's own partition-key field, and the header is left out of the [envelope](../context/envelope.md).
On delivery the subscription reports the key under the [same header](../context/headers.md).

## Example

Every shipment of one customer carries the customer as its key, so the warehouse reads them in order.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_publishing_keys.rs#keys"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_publishing_keys.rs#keys"
```

</TabItem>
</Tabs>
