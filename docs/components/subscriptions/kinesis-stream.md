---
sidebar_position: 1
title: KinesisStream
description: "KinesisStream describes one subscription by stream name, with its read pause and the shards it provisions when the stream is missing."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KinesisStream

[`KinesisStream`](api:ruststream_kinesis::KinesisStream) describes one subscription by stream name, with its read pause and the shards it provisions when the stream is missing.

[`KinesisStream::new`](api:ruststream_kinesis::KinesisStream::new) takes the stream name, and both settings are optional.
[`poll_interval`](./poll-interval.md) is the pause between reads of an idle shard, one second by default.
[`create_if_missing`](./create-if-missing.md) creates the stream with the shards it names when the service lacks it; by default a missing stream stops the service at startup.
The descriptor goes where a stream name goes: the first argument of `#[subscriber]`, or the first argument of [`subscriber(source, body)`](api:ruststream::runtime::subscriber) in the manual form.
The same two settings are also [mount-site steps](./subscriber-ext.md).
The subscription's name is the stream: a test reads its deliveries under that name, and the [AsyncAPI document](../asyncapi/channels.md) reports that channel with both settings.
A [retry copy](../settlement/retry-after.md) goes back to the same stream with no setting at the mount site, because a stream is both what a subscription reads and what a publish writes to.

## Example

Every payment of `payments` is captured; the stream is read every half second when it is quiet.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_subscriptions_stream.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_subscriptions_stream.rs#descriptor"
```

</TabItem>
</Tabs>
