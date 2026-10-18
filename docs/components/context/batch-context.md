---
sidebar_position: 1
title: KinesisBatchContext
description: "A batch handler's context carries only the seek handle, so asking for a record's position in a batch body does not compile."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KinesisBatchContext

A [batch handler's](../subscriptions/batches.md) context, [`KinesisBatchContext`](api:ruststream_kinesis::KinesisBatchContext), carries only the seek handle, so asking for a record's position in a batch body does not compile.

A batch spans many records, often from several shards, so it has no single position.
The subscription builds the context once per batch from its first record, and it holds what every record of the batch shares: the [`SeekHandle`](api:ruststream_kinesis::SeekHandle) key, the same key a [`KinesisContext`](./index.md) carries.
A batch body takes `ctx: &mut Context<'_, KinesisBatchContext>` and reads the handle with `ctx.context(SeekHandle)`.
A [seek](../positions/seek.md) from a batch body moves the whole subscription, so the next batch reads from the new position.
`Ctx<Position>` in a batch body is a compile error.

## Example

The handler drains jobs two at a time, and a batch that holds the marker job moves the subscription to the tip.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_context_batch.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_context_batch.rs#batch"
```

</TabItem>
</Tabs>
