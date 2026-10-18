---
sidebar_position: 4
description: "What each handler outcome does to a shard's checkpoint, what a restart or a handover reads again, and how a poison record is retired."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Checkpoints and delivery guarantees

This guide shows what each outcome of a handler does to a shard's checkpoint, what is read again after a restart, and how a record that can never be handled is retired.

Kinesis keeps no state per record and per reader.
A reader remembers how far it got in each shard, its checkpoint, and starts from there next time.
In RustStream the handler's outcome moves the checkpoint.

## The service

The example ships orders with a carrier.
A shipment without an address can never ship.
A shipment for a carrier this release of the service does not know waits for a release that knows it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/checkpoints.rs#ship"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/checkpoints.rs#ship"
```

</TabItem>
</Tabs>

## What each outcome does

| Outcome | The record | The shard's checkpoint |
|---|---|---|
| [`ack`](api:ruststream::runtime::HandlerOutcome::ack) | handled | moves past it once every earlier record of the shard is handled |
| [`drop`](api:ruststream::runtime::HandlerOutcome::drop) | retired, never read again | as for `ack` |
| [`retry`](api:ruststream::runtime::HandlerOutcome::retry), with no cap at the mount site | left unhandled | stops in front of it |

A checkpoint covers every record before it, so it advances only over a run of handled records.
The crate writes it to the [lease store](../components/leases/index.md) when it moves.
A record handled out of order, by a parallel lane or in a batch, moves nothing until the records before it are handled too.

`drop` is how `ship` retires a shipment without an address: another try gets the same answer.

`retry` asks Kinesis to deliver the record again, and a shard cannot do that for one record: it is a log read in order.
The delivery answers `AckError::Unsupported`, which the runtime logs as a warning, and the record stays unhandled.
`ship` holds a shipment for an unknown carrier this way.
The reader goes on: the shipments behind it are delivered and handled, and the checkpoint waits in front of the held one.
Only its own shard waits; the checkpoints of the other shards move on.
Under a cap declared at the mount site, `retry` publishes a copy instead ([Retries and dead-letter streams](retries.md)).

## What is read again

Delivery is at-least-once.
A reader that takes a shard starts after its checkpoint, so everything from the first unhandled record on is delivered again: the held shipment, and every shipment behind it, handled or not.
That happens when the service restarts, and when another instance takes the shard over ([Several instances](leases.md)).
So a handler that can see a record twice is written to be idempotent: booking the same shipment twice books it once.

The checkpoint lives in the lease store, and the store decides what survives a restart.
The default [`MemoryLeaseStore`](../components/leases/memory-lease-store.md) lives in the process and starts empty, so a restarted service opens each shard at its tip and reads only what is published from then on.
[`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md) keeps the checkpoints in a table, and a restarted service resumes where the last one stopped.
A shard with no checkpoint opens where [`start_at`](../components/positions/start-at.md) says, and at the tip when nothing says otherwise ([Replay and seeking](replay.md)).

A held record does not come back while the same reader holds the shard.
To read it again without a restart, a handler [seeks](replay.md) back to it.

## Batches and closed shards

A batch handler that returns one outcome settles every record of the batch with it.
One that returns `Vec<HandlerOutcome>` settles each record on its own, and each moves the checkpoint of its own shard.

A shard that a split or a merge closed is read to its end.
Its checkpoint becomes `SHARD_END` only after every delivery from it is settled, and only then do its children start.
A held record in a closed shard therefore holds its children back as well.

## Testing the outcomes

The tests in the example file run the app in process and check how each shipment settled.
The [in-process Kinesis](../components/testing/index.md) keeps no checkpoints, so a held record does not come back there either.
What a restart reads again runs against a stack, in the test of the [Settlement](../components/settlement/index.md) example: it restarts the service over a store that outlives the first run and sees the held record first.
