---
sidebar_position: 3
description: "Pick the shard of each record with its partition key, keep the order of each key with parallel handlers, and keep it across splits and merges."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shards, partition keys and ordering

This guide picks the shard of each record with its partition key, keeps the order of each key while handlers run in parallel, and keeps it when the stream reshards.

The example is an order service.
It posts each order to a customer's journal, four orders at a time, and confirms each journal entry with a receipt.

## How a key picks a shard

Every Kinesis record has a partition key.
Kinesis hashes the key with MD5, and the shard whose [hash key range](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html) holds the hash stores the record.
Kinesis keeps the order of the records inside one shard and keeps no order between shards.
So records with the same key stay in order, and records with different keys are spread over the shards.

One key is one shard's throughput: a shard takes up to 1 MB or 1000 records a second.
A key that carries most of the traffic makes its shard the limit of the whole stream.

## Order per customer

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#post"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#post"
```

</TabItem>
</Tabs>

The shop publishes every order with the customer as its partition key, so the orders of one customer sit on one shard in the order they were placed.
The `customer-orders` stream has two shards, and the subscription reads both side by side.

`workers(4, by_key)` runs four lanes; deliveries with the same key run one after another on one lane.
The key of a delivery is its partition key, which it carries in the `partition-key` header ([`PARTITION_KEY_HEADER`](api:ruststream_kinesis::PARTITION_KEY_HEADER)).
So `post` handles four orders at once and never two orders of one customer at the same time.
In the manual form the same setting is [`workers_by_key`](api:ruststream::runtime::SubscriberSettings::workers_by_key).
Plain `workers(4)` runs four deliveries at once with no regard to keys, and two orders of one customer can then finish in either order.

## Naming the key of a record

`post` writes each journal entry with [`partition_key(..)`](api:ruststream_kinesis::KinesisPublishSteps::partition_key), a step on the publish.
The journal entry lands on the shard of its customer, so readers of the journal see each customer in order too.
The step is a Kinesis setting, so the slot is bounded `Publisher<Options = KinesisPublishOptions>` to offer it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#confirm"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#confirm"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#app"
```

</TabItem>
</Tabs>

A reply has no call to put a step on, so the mount site names its key on the policy: [`.out_reply(Publish::default()).partition_key("receipts")`](api:ruststream_kinesis::KinesisPublishSettings::partition_key).
Every receipt then lands on one shard, and readers see the receipts in one order, at the cost of that shard's throughput.

A publish resolves its key in this order:

1. The `partition_key(..)` step of the publish.
2. The `partition-key` header the publish carries: written by hand, or brought along by a record from another broker of RustStream.
3. The key the mount site named on the policy.
4. A new key for each record, which spreads the records over the shards.

The last rung keeps no order: two records published one after the other may land on different shards.
The test of the example sets the key with the header, the portable spelling every broker of RustStream reads.

## Splits and merges

A split closes a shard and opens two children that share its hash key range.
A merge closes two adjacent shards and opens one child.
The closed parents keep their records until the retention period ends.
The subscription re-lists the shards every ten seconds.
It starts a child only after each of its parents has been read to its end, so the records of a key stay in order across the change.
A child with no checkpoint opens at its oldest record, so nothing written to it before its reader started is missed.
[Shards](../architecture/kinesis/shards.md) draws the parent and child shards, and the live tests of the crate split and merge a stream in `tests/shards_kinesis.rs`.

A [deferred retry](retries.md) publishes a copy of the record at the tip of the stream, so a retried record loses its place among the records of its key.

## The tests

The tests in the example file run the app in process.
The [in-process Kinesis](api:ruststream::testing::TestApp) gives every stream one shard, so the test that reads the orders over two shards runs against a stack:

```bash
cargo test
KINESIS_TEST_ENDPOINT=http://localhost:4566 cargo test -- --test-threads=1
```
