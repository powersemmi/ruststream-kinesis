---
title: KinesisPublish
description: "KinesisPublish, Publish in the prelude, writes each record with one PutRecord; it is the broker's default, so a reply needs no policy."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KinesisPublish

[`KinesisPublish`](api:ruststream_kinesis::KinesisPublish), `Publish` in the prelude, writes each record with one `PutRecord`; it is the broker's default, so a reply needs no policy.

A mount site names it wherever a registration publishes: `.out_reply(Publish::default())` for a reply, `.out(Slot, Publish::default())` for a slot, [`.out_retry(Publish::default())`](../settlement/out-retry.md) for retry copies.
`after_startup(Publish::default(), ..)` hands the same publisher to a startup hook.
The policy holds no connection; the runtime pairs it with the connected broker at startup into a [`KinesisPublisher`](api:ruststream_kinesis::KinesisPublisher).
A registration that names no policy for its reply publishes through this one.

A record goes to the stream its destination names: the reply type's `#[outgoing(name)]`, a `reply("stream")` clause, or `to("stream")` on the publish.
A stream is named by its name.
A publish returns once the service has stored the record, or with the [`KinesisError::Publish`](api:ruststream_kinesis::KinesisError::Publish) it reported.
The record hands the service the bytes the codec wrote, with the message's headers in the layout of the publisher's [envelope](../context/envelope.md).
A stream takes a record of up to 1 MiB, data and partition key together, unless its owner raised the limit, which goes up to 10 MiB.

Each record carries a [partition key](./keys.md), which picks its shard and its order.
A call names it with the [`partition_key`](./partition-key.md) step, and a mount site with [`KinesisPublishSettings`](./publish-settings.md).

## Example

The handler replies with a confirmation through the default policy and hands a shipment to a slot that names the policy.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_publishing_publish.rs#publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_publishing_publish.rs#publish"
```

</TabItem>
</Tabs>
