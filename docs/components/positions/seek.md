---
sidebar_position: 2
title: Seek
description: "A handler moves its running subscription through the SeekHandle key; a sequence position moves one shard, the others move every shard."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Seek

A handler moves its running subscription through the [`SeekHandle`](api:ruststream_kinesis::SeekHandle) key; a sequence position moves one shard, the others move every shard.

`Ctx<SeekHandle>` binds a [`KinesisSeeker`](api:ruststream_kinesis::KinesisSeeker) for the subscription the delivery came from.
Its `seek(position)` takes a [`KinesisPosition`](./index.md) and returns once the shards it moves read from there.
The horizon, the tip and a timestamp move every shard this instance reads.
A sequence position moves only the shard it names, and that shard needs a live reader in this instance: a shard another instance reads, or one already finished, is refused with [`KinesisError::Read`](api:ruststream_kinesis::KinesisError::Read).
[`Ctx<Position>`](../context/index.md) gives the delivery's own sequence position to aim with.

A seek moves this instance and no other.
It writes no [checkpoint](../settlement/index.md): it starts the checkpoint bookkeeping of the shards it moves afresh, so an acknowledgement of a record delivered before the seek no longer moves the checkpoint.
The next acknowledgement after the seek checkpoints from the new position.
Records from that position onward are delivered again, which at-least-once delivery allows.
A seek after the service shut down returns [`KinesisError::NotConnected`](../broker/shutdown.md).
A batch handler reaches the same key through its [`KinesisBatchContext`](../context/batch-context.md).
A position that has to apply on every startup belongs in [`start_at`](./start-at.md) instead.

## Example

The handler replays the retained backlog, and the marker job moves the subscription to the tip, so the rest of the backlog is skipped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_seek.rs#seek"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_seek.rs#seek"
```

</TabItem>
</Tabs>
