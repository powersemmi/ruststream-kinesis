---
sidebar_position: 6
description: "Read a Kinesis stream again from its oldest record or from a moment, and move a running subscription from inside its handler."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Replay and seeking

This guide reads a stream again from its oldest record or from a moment, and moves a running subscription from inside its handler.

Kinesis keeps a record after a consumer has read it, until the stream's retention period ends.
A service can read it again: to rebuild a read model, to backfill a new consumer, or to skip records it cannot use.
A subscription takes its start position at the mount site, and a handler moves its own subscription through the delivery's context.

## The read models

The service keeps two read models in memory: the balance of every account and the spend of every card.
Each posting carries the ledger's revision.
A correction of past postings is published as a new revision, and a read model that meets one rebuilds itself from the oldest record of the stream.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#projection"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#projection"
```

</TabItem>
</Tabs>

## Start from the oldest record

The balances live in memory, so after a restart they are built again from the whole stream.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#balance"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#balance"
```

</TabItem>
</Tabs>

[`start_at(KinesisPosition::horizon())`](../components/positions/start-at.md) opens every shard at its trim horizon, the oldest record the stream still keeps.
It applies on every start, to the shards the subscription opens with, ahead of any stored [checkpoint](checkpoints.md).
Without `start_at`, a shard resumes after its checkpoint and opens at its tip when it has none.

[`KinesisPosition`](../components/positions/index.md) has four positions:

| Position | Where the subscription reads from |
|---|---|
| [`horizon()`](api:ruststream_kinesis::KinesisPosition::horizon) | the oldest record of every shard |
| [`latest()`](api:ruststream_kinesis::KinesisPosition::latest) | the tip of every shard, so only new records are read |
| [`timestamp(ms)`](api:ruststream_kinesis::KinesisPosition::timestamp) | the first record of every shard that arrived at or after `ms` |
| [`sequence(shard, seq)`](api:ruststream_kinesis::KinesisPosition::sequence) | one record of one shard, and the records after it |

A stream keeps its records for 24 hours by default, and up to 365 days with [extended retention](https://docs.aws.amazon.com/streams/latest/dev/kinesis-extended-retention.html).
What is past the retention period is gone, and the trim horizon moves forward with it.

## Seek from a handler

The same handler rebuilds the balances when a posting of a newer revision arrives.
[`Ctx<SeekHandle>`](../components/positions/seek.md) gives the handler the [`KinesisSeeker`](api:ruststream_kinesis::KinesisSeeker) of its own subscription.
[`seek`](api:ruststream::Seeker::seek) moves the subscription to the oldest record, and the handler reads every posting again.
The posting that started the rebuild comes back as well, and this time the projection applies it.

A seek takes any of the positions above.
A position for the whole stream moves every shard this instance holds.
`Ctx<Position>` holds the delivery's own position, a `sequence` position: a seek there reads that shard again from the record and moves no other shard.
It needs this instance to hold the shard; a seek to a shard that another instance reads, or that is finished, fails.

## Seek from a batch handler

Card postings arrive in volume, so the spend is built in [batches](../components/subscriptions/batches.md) of up to 500.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#batch"
```

</TabItem>
</Tabs>

A batch handler declares [`KinesisBatchContext`](../components/context/batch-context.md) instead of the per-delivery context.
It carries the same `SeekHandle` key and no position, because a batch spans many records.
A body that needs the place of one record reads its `kinesis-shard-id` and `kinesis-sequence-number` headers.

## Start from a moment

The payout review reads the payouts of the last day on every start.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#payouts"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#payouts"
```

</TabItem>
</Tabs>

The moment is known only when the service starts, so the mount site names the position instead of the attribute.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/replay.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#service"
```

</TabItem>
</Tabs>

`KinesisPosition::timestamp` takes milliseconds since the epoch.
Kinesis matches it against the arrival timestamp it stamped on each record.
That stamp is approximate, so records that arrived around the same moment are not told apart exactly.

## Seeks and checkpoints

A seek clears the checkpoint bookkeeping of every shard it moves.
A delivery read before the seek settles into nothing, so the ack of the posting that started the rebuild moves no checkpoint.
A later checkpoint therefore never passes a record that the seek replayed and no handler has handled yet.
A seek moves this instance only: other instances keep reading the shards they hold, and an instance that takes a shard over resumes from its checkpoint.

The tests in the example file run the app in process.
The [in-process Kinesis](../components/testing/index.md) keeps the whole log of each stream, so a seek reads it again there too.
