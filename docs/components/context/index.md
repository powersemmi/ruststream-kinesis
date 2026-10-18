---
title: KinesisContext
description: "Context<'_, KinesisContext> gives a handler the record's position and the seek handle, through the Position and SeekHandle keys."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KinesisContext

`Context<'_, KinesisContext>` gives a handler the record's position and the seek handle, through the [`Position`](api:ruststream_kinesis::Position) and [`SeekHandle`](api:ruststream_kinesis::SeekHandle) keys.

The subscription builds a [`KinesisContext`](api:ruststream_kinesis::KinesisContext) once per delivery, beside the payload and the [headers](./headers.md).
Building it costs three reference-count increments: the shard id and the sequence number are shared with the delivery, and the seeker is the subscription's own.
A handler binds one field with [`Ctx<K>`](api:ruststream::runtime::Ctx), where `K` is one of the two keys:

| Key | Value | What it is |
|---|---|---|
| `Position` | [`KinesisPosition`](../positions/index.md) | the record's own place: its shard and its sequence number, as `KinesisPosition::Sequence` |
| `SeekHandle` | [`KinesisSeeker`](api:ruststream_kinesis::KinesisSeeker) | the handle that [moves the subscription](../positions/seek.md) |

Both keys are in the prelude.
A body that reads both takes `ctx: &mut Context<'_, KinesisContext>` instead and calls `ctx.context(key)` per field, as the manual form does.
A key the context does not carry is a compile error, not a lookup that finds nothing.
A [batch handler](./batch-context.md) has a context of its own, with the seek handle only.

## Example

The handler writes where each order sits in the stream to an audit trail.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_context.rs#context"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_context.rs#context"
```

</TabItem>
</Tabs>
