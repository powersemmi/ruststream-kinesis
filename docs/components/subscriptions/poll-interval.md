---
sidebar_position: 2
title: poll_interval
description: "poll_interval sets the pause between reads of an idle shard, one second by default; the service allows five reads a second per shard."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# poll_interval

[`poll_interval`](api:ruststream_kinesis::KinesisStream::poll_interval) sets the pause between reads of an idle shard, one second by default; the service allows five reads a second per shard.

Each shard reader asks `GetRecords` for the next records of its shard.
When a read returns records, the reader reads again at once; when a read comes back empty, it waits `poll_interval` first.
So the setting bounds how long a new record waits on a quiet shard, and how often a quiet shard is read.
The five reads a second are a limit of the shard, shared by every consumer that reads it.
A read over that limit is throttled; the reader then waits one interval and reads on from where it stopped, and the handler sees a pause rather than an error.
A shorter pause lowers the latency on a quiet stream and spends more of the shared limit.
The same setting is a mount-site step through [`KinesisSubscriberExt`](./subscriber-ext.md).
The [AsyncAPI document](../asyncapi/channels.md) reports it on the channel as `pollIntervalMs`.

## Example

Quotes are repriced within 200 ms of their arrival on a quiet stream.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_subscriptions_poll.rs#poll"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_subscriptions_poll.rs#poll"
```

</TabItem>
</Tabs>
