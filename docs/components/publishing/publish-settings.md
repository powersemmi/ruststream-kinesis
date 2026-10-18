---
sidebar_position: 3
title: KinesisPublishSettings
description: "partition_key at the mount site keys every record of a position, a reply included, which has no publish call to put a step on."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KinesisPublishSettings

`partition_key` at the mount site keys every record of a position, a reply included, which has no publish call to put a step on.

A replying handler returns a value, and the runtime publishes it.
So a reply has no publish builder for the [`partition_key`](./partition-key.md) step.
[`KinesisPublishSettings::partition_key`](api:ruststream_kinesis::KinesisPublishSettings::partition_key) names the key on the policy instead, right after the position it applies to: `.out_reply(Publish::default()).partition_key("receipts-v1")`, or after `.out(Slot, ..)`.
[`KinesisPublish::partition_key`](api:ruststream_kinesis::KinesisPublish::partition_key) is the same setting written on the policy value.
The setting is available after this crate's policy and after no other broker's.

The key ranks third among the [places a key comes from](./keys.md), so a step or a `partition-key` header on a record wins over it.
A retry copy carries the delivery's headers, its `partition-key` among them, so on the [retry position](../settlement/out-retry.md) the copy keeps the record's own key.
A reply that needs a key of its own per delivery takes a publish transform at the mount site that writes `partition_key` into the options.
The [AsyncAPI document](../asyncapi/messages.md) reports the key on the message object.

## Example

Every receipt carries one key, so all receipts land on one shard, in order.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_replies.rs#replies"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_replies.rs#replies"
```

</TabItem>
</Tabs>
