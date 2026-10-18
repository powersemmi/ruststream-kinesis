---
sidebar_position: 4
title: owner_id
description: "owner_id names this instance in the lease table; left out, each broker takes an id of its own, unique to that broker."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# owner_id

[`owner_id`](api:ruststream_kinesis::KinesisBroker::owner_id) names this instance in the lease table; left out, each broker takes an id of its own, unique to that broker.

A [lease store](./index.md) records which owner holds each shard, and grants an owner the leases it already holds.
So an instance that restarts under the same id takes its shards back at once, without waiting for its old leases to expire.
A stable name per instance, such as a pod name, gives that.
A fixed id must never be shared by two instances that run at the same time: both would hold the same shards, and each would read them.
The [`DynamoLeaseStore`](./dynamo-lease-store.md) table shows the id in the `lease_owner` column of each row, which tells an operator which instance reads which shard.

## Example

The dispatch service names each instance after its host.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_leases_owner.rs#owner"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_leases_owner.rs#owner"
```

</TabItem>
</Tabs>
