---
sidebar_position: 5
title: Shards
description: "A subscription lists shards every ten seconds, leases each one, reads it with one reader, and starts a split's children after their parents."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shards

A subscription lists its stream's shards every ten seconds, leases each one, reads it with one reader, and starts a split's children after their parents.

A Kinesis stream is a set of shards, and the partition key of a record picks its shard: the service hashes the key, so one key always lands on one shard.
Records of one shard are read in the order they were written, so records that share a key keep their order.
Records of different shards have no order between them, and one subscription reads its shards side by side.
For each shard the subscription takes a lease in the [lease store](../leases/index.md); a shard another instance holds is left to that instance.
The listing repeats every ten seconds, so a shard that a split or a merge adds is picked up within that time.
A child shard starts only after every parent is fully read and checkpointed as finished, which keeps the order of a key across resharding.
A parent the subscription never read, while it opens at the tip, does not hold its children back.
How a publish names the key is on the [Keys](../publishing/keys.md) page.
The in-process transport of a [`TestApp`](api:ruststream::testing::TestApp) test keeps one shard per stream, so there every record is ordered against every other.

## Example

The stream has two shards, and each customer's shipments carry the customer as their key, so they arrive in order.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_subscriptions_shards.rs#shards"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_subscriptions_shards.rs#shards"
```

</TabItem>
</Tabs>
