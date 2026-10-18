---
sidebar_position: 3
title: legacy_stream
description: "legacy_stream names the stream a table's bare shard rows belong to, so a table written by an earlier version resumes from them."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# legacy_stream

[`legacy_stream`](api:ruststream_kinesis::DynamoLeaseStore::legacy_stream) names the stream a table's bare shard rows belong to, so a table written by an earlier version resumes from them.

Earlier versions of this crate keyed a row by the shard id alone, such as `shardId-000000000000`.
A bare row names no stream, and every stream's first shard has that id.
So [`DynamoLeaseStore`](./dynamo-lease-store.md) reads a bare row only for the stream `legacy_stream` names.
A shard of that stream with no checkpoint under its own `stream:shard` row resumes from its bare row, and the first read copies the checkpoint to the new row.
A checkpoint already on the new row wins over the bare one.
Other streams ignore the bare rows.
Without `legacy_stream`, a shard whose bare row holds a checkpoint is refused with a lease error that names the table, the shard and the stream, and it is not read until the stream is named or the row is deleted.
Once the upgraded service has opened a subscription on every stream it reads, every bare row with progress has its copy; the rows whose `lease_key` holds no `:` can then be deleted, and the setting dropped.
A table that served several streams before the upgrade holds, in each bare row, the checkpoint of whichever stream wrote last.
No stream can resume from such a row: delete the bare rows and open each stream with [`start_at`](../positions/start-at.md) at the position it should read from.
The two versions lease different rows, so an old and a new instance can read one shard at once; stop the old instances before the new ones start.

## Example

The table `returns-leases` served the stream `returns` alone before the upgrade.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_leases_legacy.rs#legacy"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_leases_legacy.rs#legacy"
```

</TabItem>
</Tabs>
