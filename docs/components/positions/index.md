---
title: KinesisPosition
description: "KinesisPosition names a place in the stream: the trim horizon, the tip, an arrival timestamp on every shard, or one record of one shard."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KinesisPosition

[`KinesisPosition`](api:ruststream_kinesis::KinesisPosition) names a place in the stream: the trim horizon, the tip, an arrival timestamp on every shard, or one record of one shard.

A stream keeps its records for its retention period, so a subscription can be moved through them, at startup with [`start_at`](./start-at.md) or from a handler with a [seek](./seek.md).
By default each shard resumes from its [checkpoint](../settlement/index.md), and a shard without one opens at the tip.

| Position | Scope | Meaning |
|---|---|---|
| `KinesisPosition::horizon()` | every shard | the trim horizon: the oldest record the stream still retains |
| `KinesisPosition::latest()` | every shard | the tip: only records published from now on |
| `KinesisPosition::timestamp(millis)` | every shard | on each shard, the first record that arrived at or after that instant, in milliseconds since the Unix epoch |
| `KinesisPosition::sequence(shard, sequence)` | one shard | exactly one record, by its shard id and sequence number |

The first three apply to every shard the subscription reads.
A timestamp is matched against the arrival time the service stamped on each record, which the service calls approximate: records that arrived within the same moment are not told apart.
A sequence position names one shard and moves no other.
A delivery's own place is a sequence position: [`Ctx<Position>`](../context/index.md) binds it, and a seek there reads that record again.
The type is also what the core's `Positioned` returns for a delivered record.

## Example

A report is rebuilt from the readings that arrived since an instant its configuration names.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_positions_timestamp.rs#replay"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_positions_timestamp.rs#replay"
```

</TabItem>
</Tabs>
