---
sidebar_position: 5
title: Prelude
description: "One glob, ruststream_kinesis::prelude, brings the core prelude, the broker, the descriptor, positions, context keys and the publish surface."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

One glob, [`ruststream_kinesis::prelude`](api:ruststream_kinesis::prelude), brings the core prelude, the broker, the descriptor, positions, context keys and the publish surface.

`use ruststream_kinesis::prelude::*;` is the one import a Kinesis service writes.
It carries the core's own prelude, so `#[subscriber]`, `RustStream`, `HandlerOutcome` and `Out` come with it.
It adds [`KinesisBroker`](./index.md), the descriptor [`KinesisStream`](../subscriptions/kinesis-stream.md) with its mount-site steps [`KinesisSubscriberExt`](../subscriptions/subscriber-ext.md), and [`KinesisPosition`](../positions/index.md) with the core's `Seeker` and `Positioned`.
It adds the [contexts](../context/index.md) `KinesisContext` and `KinesisBatchContext` with their keys `Position` and `SeekHandle`, and the seeker type `KinesisSeeker`.
With the `dynamodb-lease` feature it adds [`DynamoLeaseStore`](../leases/dynamo-lease-store.md).

The publish policy comes under the name every broker crate uses: `Publish` is [`KinesisPublish`](../publishing/index.md).
So a mount site reads the same on every broker.
Two broker preludes in one file collide on that name; the prefixed type at the crate root tells them apart.
The prelude also brings [`KinesisPublishSettings`](../publishing/publish-settings.md), [`KinesisPublishSteps`](../publishing/partition-key.md) and `KinesisPublishOptions`.

A handler body bounds its slots with capability traits, such as `Out<impl Publisher, Slot>`, and names no broker type.
The one exception is a body that names a record's [partition key](../publishing/partition-key.md): it needs `KinesisPublishSteps` in scope and the slot bounded `Out<impl Publisher<Options = KinesisPublishOptions>, Slot>`, both from this prelude.

## Example

The descriptor, its read pause, the start position, the context key and the reply policy with its key all come from the one glob.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_broker_prelude.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_broker_prelude.rs#app"
```

</TabItem>
</Tabs>
