---
sidebar_position: 7
description: "Run several instances of a Kinesis service on one stream: a lease table in DynamoDB gives each shard to one instance and keeps its checkpoint."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Several instances

This guide runs several instances of one service on one stream, with a lease table in DynamoDB that gives each shard to one instance and keeps its checkpoint.

Kinesis does not decide which consumer reads which shard.
The Kinesis Client Library of other languages keeps a [lease table](https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html) for it, one row per shard with its owner and checkpoint.
`ruststream-kinesis` keeps the same kind of table through a [lease store](../components/leases/index.md).

## One instance

A broker that names no store keeps its leases and checkpoints in [`MemoryLeaseStore`](../components/leases/memory-lease-store.md), in the memory of the process.
That is right for one instance.
Two instances on it do not know of each other, so each reads every shard and every record is handled twice.
A restarted instance starts with an empty store and opens each shard at its tip, unless `start_at` names another position.

## The lease table

```toml title="Cargo.toml"
[dependencies]
ruststream-kinesis = { version = "0.7", features = ["dynamodb-lease"] }
aws-config = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The `dynamodb-lease` feature adds [`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md).
Its table has one string partition key, `lease_key`, and nothing else; on-demand billing fits it.
The table is infrastructure, like the stream: create it once, with Terraform, CloudFormation or the AWS CLI.
On the local stack, add `dynamodb` to `SERVICES` in the compose file and create it there:

```bash
aws dynamodb create-table --table-name warehouse-leases \
  --attribute-definitions AttributeName=lease_key,AttributeType=S \
  --key-schema AttributeName=lease_key,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

One table serves every stream of a service: a row holds one shard of one stream, under the key `stream:shard`.

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/leases.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/leases.rs#service"
```

</TabItem>
</Tabs>

The store and the broker take one AWS configuration, so both reach the same account and region.
`aws_config::defaults(..).load()` reads it from the environment, and it is `async`, so `main` loads it before it builds the app and runs it with `run`.
[`KinesisBroker::from_config`](api:ruststream_kinesis::KinesisBroker::from_config) takes the configuration, and [`lease_store`](api:ruststream_kinesis::KinesisBroker::lease_store) the store.
Run two copies of the service with the same environment, and they share the shards of `warehouse-orders`.

## How the shards are shared

Each instance lists the shards when its subscription opens and again every ten seconds.
It takes the lease of every shard that has no owner, or whose lease has expired, and reads those shards.
A lease lasts ten seconds, and its holder renews it every three.
Every write to the table is conditional, so two instances never hold one shard at once, and a checkpoint lands only while its writer holds the lease.

The instances do not balance the shards between them: the one that lists first takes every free shard.
The others take a shard when its holder stops, which releases its leases, or when its lease expires because the holder crashed or lost the network.
An instance whose renewal fails stops reading that shard at once, without writing a checkpoint.

## What a handover reads again

The instance that takes a shard over resumes after the checkpoint the last holder wrote.
Records the last holder handled after its last checkpoint are delivered again, and so is a record it [held](checkpoints.md) and everything behind it.
A restarted instance resumes the same way, since the checkpoints live in the table.

## Naming each instance

Each broker takes an owner id of its own, unique to that broker instance, and the table shows it in the `lease_owner` column.
[`owner_id`](../components/leases/owner-id.md) names the instance instead, for example after its pod.
An instance that restarts under the same name gets its leases back at once, without waiting for them to expire.
A fixed name must never be shared by two instances that run at the same time: both would hold the same shards and read them.

## Upgrading a table, and other stores

A table written by an earlier version of the crate keys its rows by the shard id alone.
[`legacy_stream`](../components/leases/legacy-stream.md) names the stream those rows belong to, so the upgraded service resumes from them.
A store on another database implements [`LeaseStore`](api:ruststream_kinesis::LeaseStore): five calls that acquire, renew, checkpoint, read and release a lease.

## Testing

The first test starts the app in process with a configuration built without I/O.
The [in-process Kinesis](api:ruststream::testing::TestApp) consults no lease store, so it tests the handler.
The second test runs against a stack: it starts two instances on one new table, stops the first and sees the second take the shard over after the first one's checkpoint.

```bash
KINESIS_TEST_ENDPOINT=http://localhost:4566 cargo test -- --test-threads=1
```

The crate's own live tests of the lease table are in `tests/leases_kinesis.rs`.
