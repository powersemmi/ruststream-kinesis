---
sidebar_position: 3
title: create_if_missing
description: "create_if_missing creates the stream with the shard count it names when it is missing, then waits up to a minute for it to become active."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# create_if_missing

[`create_if_missing`](api:ruststream_kinesis::KinesisStream::create_if_missing) creates the stream with the shard count it names when it is missing, then waits up to a minute for it to become active.

The check runs once, when the subscription opens at startup.
A stream the service already has is left as it is, whatever its shard count.
A missing one is created with `CreateStream`, and the subscription opens once the stream is active.
Two instances that start together may both try to create it; the one that loses the race uses the stream the other created.
A stream that is not active within the minute, or a creation the service refuses, stops the service at startup with a [`KinesisError::Stream`](api:ruststream_kinesis::KinesisError::Stream) that names the stream.
A shard count below one is refused before any request, also at startup.
Without this setting a missing stream stops the service at startup, which catches a mistyped stream name before the service runs.
The setting suits local stands and tests; a production stream is usually managed as infrastructure.
The service's credentials need the `kinesis:CreateStream` permission for it.
The in-process transport of a [`TestApp`](api:ruststream::testing::TestApp) test has every stream a service names, so there the setting changes nothing.
The [AsyncAPI document](../asyncapi/channels.md) reports the shard count on the channel as `shardCount`.

## Example

On a stand without the stream `parcels`, the subscription creates it with two shards.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_subscriptions_create.rs#create"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_subscriptions_create.rs#create"
```

</TabItem>
</Tabs>
