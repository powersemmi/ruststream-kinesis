---
sidebar_position: 2
title: Headers
description: "Every delivery carries its sequence number, its shard and its partition key as headers, where the framework's per-key dispatch finds the key."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers

Every delivery carries its sequence number, its shard and its partition key as headers, where the framework's per-key dispatch finds the key.

| Header | Constant | Value |
|---|---|---|
| `kinesis-sequence-number` | [`SEQUENCE_HEADER`](api:ruststream_kinesis::SEQUENCE_HEADER) | the record's sequence number on its shard, a decimal string too wide for an integer |
| `kinesis-shard-id` | [`SHARD_HEADER`](api:ruststream_kinesis::SHARD_HEADER) | the shard the record was read from, such as `shardId-000000000000` |
| `partition-key` | [`PARTITION_KEY_HEADER`](api:ruststream_kinesis::PARTITION_KEY_HEADER) | the record's [partition key](../publishing/keys.md) |

The subscription writes the three headers from the record's own fields, next to the headers the producer wrote in the [envelope](./envelope.md).
A handler reads them from `ctx.headers()`, as text with `get_str`.
The sequence number and the shard name the same record that the [`Position`](./index.md) key names.

The partition key under `partition-key` is the key the core's `IncomingMessage::partition_key` reports.
So `workers(n, by_key)` runs the records of one key one after another on one lane, in the order the shard holds them, while records of other keys run beside them.
A [retry copy](../settlement/retry-after.md) carries the delivery's headers, so it keeps the record's partition key.

## Example

The meter is each reading's partition key, so the readings of one meter are billed in order on four lanes.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_context_headers.rs#headers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_context_headers.rs#headers"
```

</TabItem>
</Tabs>
