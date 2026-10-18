---
sidebar_position: 1
title: start_at
description: "start_at opens the subscription at a position you name on every startup, ahead of the stored checkpoints."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# start_at

`start_at` opens the subscription at a [position](./index.md) you name on every startup, ahead of the stored checkpoints.

It is a clause of the attribute, `start_at(KinesisPosition::horizon())`, or [`.start_at(position)`](api:ruststream::runtime::SubscriberSettings::start_at) among the mount-site settings.
The position lands on the shards the subscription opens with, ahead of their first read, whatever their [checkpoints](../settlement/index.md) say.
A reader that starts later in the run, after a failure or a takeover, resumes from its shard's checkpoint.
A child shard of a split or a merge with no checkpoint of its own opens at its oldest record, so no record of a resharded key is skipped.
A sequence position on a shard this instance does not read is refused, and the service stops at startup.
It suits a read model rebuilt from the retained stream on each boot.

## Example

The audit projection is rebuilt from the oldest retained entry on every startup, although it checkpointed every entry it read before.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_positions_start_at.rs#start_at"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_positions_start_at.rs#start_at"
```

</TabItem>
</Tabs>
