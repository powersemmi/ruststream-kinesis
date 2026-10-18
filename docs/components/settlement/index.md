---
title: Checkpoint
description: "An ack is a per-shard checkpoint: it moves only when every earlier record is handled; drop retires a record, and a requeue is unsupported."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Settlement

An ack is a per-shard checkpoint: it moves only when every earlier record is handled; [`drop`](api:ruststream::runtime::HandlerOutcome::drop) retires a record, and a requeue is unsupported.

A shard is an ordered log, and the service keeps no per-record state for a reader.
So the subscription remembers one position per shard, the checkpoint, in the [lease store](../leases/index.md).
[`HandlerOutcome::ack`](api:ruststream::runtime::HandlerOutcome::ack) marks the record handled.
The checkpoint moves past it once every earlier record of the shard is handled too, and the lease store records the new position.
`HandlerOutcome::drop` marks the record handled the same way, which is how a record the handler can never process is retired.
[`HandlerOutcome::retry`](api:ruststream::runtime::HandlerOutcome::retry) asks for a requeue, which a shard cannot perform; the runtime logs the refusal as a warning.
The record stays unhandled, and the checkpoint stops in front of it while later records are still delivered and handled.
It is read again, with every record after it, when the shard's lease is next taken: after a restart or when another instance takes the shard over.
That needs a store that keeps checkpoints across a restart, such as [`DynamoLeaseStore`](../leases/dynamo-lease-store.md); the default [`MemoryLeaseStore`](../leases/memory-lease-store.md) forgets them, and a restarted service opens each shard at the tip.
Within one run the record does not come back.
A registration that declares [`max_attempts`](./dead-letters.md) turns `retry` into a counted copy published back to the stream, as [`retry_after`](./retry-after.md) does after a delay.
Delivery is at-least-once: a record handled after the held one is delivered again when the shard is read from the checkpoint.
A [seek](../positions/seek.md) starts the checkpoint bookkeeping of the shards it moves afresh.
In a [`TestApp`](api:ruststream::testing::TestApp) test the in-process transport consults no lease store, and a held record does not come back there either.

## Example

An entry is booked, an empty one is retired, and a refund holds the checkpoint until the payment it returns is booked.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_settlement_outcomes.rs#outcomes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_settlement_outcomes.rs#outcomes"
```

</TabItem>
</Tabs>
