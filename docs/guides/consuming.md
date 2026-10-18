---
sidebar_position: 2
description: "Read a Kinesis stream one record or one batch at a time, spend the shard's read budget with poll_interval and the batch size, and know what a delivery carries."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Consuming a stream

This guide reads a stream one record and one batch at a time, spends each shard's read budget, and shows what a delivery carries.

The example is an analytics service.
It books payments as they come and counts page views in batches.

## How a stream is read

A subscription runs one reader per shard it holds.
The reader calls `GetRecords` with the shard's iterator, hands the records over in order and asks again at once.
After a read that returned nothing it pauses for [`poll_interval`](../components/subscriptions/poll-interval.md), one second by default, before it asks again.
At most 64 deliveries wait between the readers and the handler; a reader that finds the queue full waits, so a slow handler slows the reads down.

Kinesis allows each shard [five `GetRecords` calls a second and 2 MB a second](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html).
Every consumer of the stream shares that budget.
A service that reads too often is throttled: Kinesis answers `ProvisionedThroughputExceededException`.
The AWS SDK retries a throttled call on its own first.
When the retries run out, the reader waits one `poll_interval` and asks again with the same iterator.
The handler sees a pause, and no error and no redelivery.

## One record at a time

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#payments"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#payments"
```

</TabItem>
</Tabs>

`book` reads the `payments` stream and settles each payment on its own.
A single subscription asks for up to 1000 records per read.
`poll_interval(Duration::from_millis(250))` shortens the pause after an empty read, so a new payment waits at most a quarter of a second.
Four empty reads a second leave one read a second of the shard's budget to the other consumers.
The setting is also a [mount-site step](../components/subscriptions/subscriber-ext.md), `.poll_interval(..)` after `b.include(..)`.

[`Ctx<Position>`](../components/context/index.md) reads the record's place in the stream: its shard and sequence number, as a [`KinesisPosition`](../components/positions/index.md).
A seek to that position reads the shard again from that record ([Replay and seeking](replay.md)).
Every delivery also carries three [headers](../components/context/headers.md):

| Header | Value |
|---|---|
| `kinesis-shard-id` ([`SHARD_HEADER`](api:ruststream_kinesis::SHARD_HEADER)) | the shard the record was read from |
| `kinesis-sequence-number` ([`SEQUENCE_HEADER`](api:ruststream_kinesis::SEQUENCE_HEADER)) | its sequence number in that shard |
| `partition-key` ([`PARTITION_KEY_HEADER`](api:ruststream_kinesis::PARTITION_KEY_HEADER)) | the partition key it was published with |

A handler reads them with `ctx.headers()`.
The other headers of a record are the ones its producer wrote.

## Batches

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#views"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#views"
```

</TabItem>
</Tabs>

A handler over a slice is a [batch handler](../components/subscriptions/batches.md).
The app mounts `count` with `.batch(nonzero!(500))`, and 500 becomes the limit of every `GetRecords` call of its readers.
One read then never fetches more than one batch, and Kinesis caps a read at 10 000 records.
A batch that is not full goes out 50 ms after its first record, so a quiet stream still delivers its records quickly.
One batch can mix records of every shard the instance holds.

The size is a trade between latency and the read budget.
Small batches mean small reads, and each read spends one of the shard's five calls a second.
`count` settles the whole batch with one outcome.
A handler that returns `Vec<HandlerOutcome>` settles each record on its own, and each record then moves the checkpoint of its own shard ([Checkpoints and delivery guarantees](checkpoints.md)).

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#app"
```

</TabItem>
</Tabs>

A handler runs one delivery at a time by default.
`workers(n)` in the attribute, or the `workers` step of the manual form, runs up to `n` side by side; [Shards, partition keys and ordering](ordering.md) shows how to keep the order of each key while doing so.

The crate reads through the stream's shared throughput, with `GetRecords`.
A record that the Kinesis Producer Library aggregated is not handed to a handler: the subscription reports it as [`KinesisError::AggregatedRecord`](../components/broker/errors.md) and reads on.

The tests in the example file run the app in process.
The batch test starts on a paused clock, so the 50 ms a partial batch waits pass at once.
