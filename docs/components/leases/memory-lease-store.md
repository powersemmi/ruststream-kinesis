---
sidebar_position: 1
title: MemoryLeaseStore
description: "MemoryLeaseStore is the default store: correct for one service instance, and empty again after a restart."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MemoryLeaseStore

[`MemoryLeaseStore`](api:ruststream_kinesis::MemoryLeaseStore) is the default store: correct for one service instance, and empty again after a restart.

A broker that names no [lease store](./index.md) creates one with [`MemoryLeaseStore::new`](api:ruststream_kinesis::MemoryLeaseStore::new).
It keeps leases and checkpoints in the memory of the process, so one instance reads every shard of its streams and orders a split's children after their parents.
Brokers of one process that share it through an `Arc` share its leases and checkpoints.
A new process starts with an empty store.
So a restarted service opens each shard at the tip, unless [`start_at`](../positions/start-at.md) names another position.
The records published while the service was down are not read, and a [held record](../settlement/index.md) does not come back.
A service that must resume where it stopped, or runs in more than one instance, uses a store outside the process, such as [`DynamoLeaseStore`](./dynamo-lease-store.md).

## Example

The audit service runs as one instance and reads from the tip after each start.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_leases_memory.rs#memory"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_leases_memory.rs#memory"
```

</TabItem>
</Tabs>
