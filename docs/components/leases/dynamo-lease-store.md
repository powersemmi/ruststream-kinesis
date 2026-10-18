---
sidebar_position: 2
title: DynamoLeaseStore
description: "DynamoLeaseStore keeps leases in a DynamoDB table, so several instances share the shards; the table needs only a string key lease_key."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# DynamoLeaseStore

[`DynamoLeaseStore`](api:ruststream_kinesis::DynamoLeaseStore) keeps leases in a DynamoDB table, so several instances share the shards; the table needs only a string key `lease_key`.

It needs the `dynamodb-lease` feature of the crate.
[`DynamoLeaseStore::new`](api:ruststream_kinesis::DynamoLeaseStore::new) takes the AWS configuration and the table name.
The table has a string partition key named `lease_key` and nothing else; on-demand billing fits it.
One row holds one shard of one stream, under the key `stream:shard`, so one table serves every stream of a service.
The row holds the owner, the lease's expiry, the checkpoint, the first record of the shard's last reading (`opened_at`) and a counter bumped on every write.
A row written by version 0.7.1 or earlier has no `opened_at`, and a shard with no checkpoint there opens at the subscription's start position, as it did then.
Every write is conditional, so two instances never both hold a shard, and a checkpoint lands only while its writer holds the lease.
The checkpoints outlive the process, so a restarted instance resumes each shard where its acknowledgements stopped.
The broker is built from the same configuration with [`KinesisBroker::from_config`](api:ruststream_kinesis::KinesisBroker::from_config), so both reach one account and region.
The service's credentials need `dynamodb:GetItem` and `dynamodb:UpdateItem` on the table.
Each instance needs an [owner id](./owner-id.md) of its own; a table written by an earlier version of this crate is read through [`legacy_stream`](./legacy-stream.md).

## Example

Every instance of the fulfilment service runs this code against one table.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_leases.rs#leases"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_leases.rs#leases"
```

</TabItem>
</Tabs>
