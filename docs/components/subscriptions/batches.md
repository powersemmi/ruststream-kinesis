---
sidebar_position: 6
title: Batches
description: "A slice handler gets up to batch(n) records; n becomes the GetRecords limit, and a short batch goes out 50 ms after its first record."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler gets up to `batch(n)` records; `n` becomes the `GetRecords` limit, and a short batch goes out 50 ms after its first record.

A handler over `&[T]` is a batch handler, and [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is its largest batch.
Every shard reader asks `GetRecords` for at most `n` records, so one read never fetches more than one batch holds; the service caps a read at 10,000 records.
A batch that is still filling goes to the handler 50 ms after its first record, so on a quiet stream a batch is often smaller than `n`.
One batch mixes the records of every shard this instance reads.
One outcome settles every record of the batch.
A `Vec<HandlerOutcome>` settles record `i` with outcome `i`, and each record moves the [checkpoint](../settlement/index.md) of its own shard.
A [`retry_after`](../settlement/retry-after.md) in a batch becomes a copy published back to the stream after the delay.
A small batch means small reads against the same five reads a second per shard, which [`poll_interval`](./poll-interval.md) paces.
A batch body reads a [`KinesisBatchContext`](../context/batch-context.md), which carries the seek handle and no position of a single record.
In the manual form the body is an `impl Handle<[T]>`, where `Ok(())` acks the batch and `Err(outcomes)` settles record by record.

## Example

Orders are stored up to 500 at a time; refunds are reviewed up to 100 at a time.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_batches.rs#batches"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_batches.rs#batches"
```

</TabItem>
</Tabs>

## One outcome per record

Only the unapproved refund comes back, as a copy published after the delay.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_batches.rs#per_record"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_batches.rs#per_record"
```

</TabItem>
</Tabs>
