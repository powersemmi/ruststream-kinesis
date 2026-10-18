---
sidebar_position: 2
title: partition_key
description: "The partition_key step keys one record on the publish builder; a body that names it bounds its slot on KinesisPublishOptions."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# partition_key

The [`partition_key`](api:ruststream_kinesis::KinesisPublishSteps::partition_key) step keys one record on the publish builder; a body that names it bounds its slot on [`KinesisPublishOptions`](api:ruststream_kinesis::KinesisPublishOptions).

The step stands among the framework's own: `.message(..)`, `.to(..)`, `.partition_key(..)`, `.publish()`.
It sets the key of this record only, and it wins over every other place a [key](./keys.md) can come from.
The step comes from [`KinesisPublishSteps`](api:ruststream_kinesis::KinesisPublishSteps) in the [prelude](../broker/prelude.md).
It is reachable where the publisher's options are Kinesis's, so the body bounds its slot as `Out<impl Publisher<Options = KinesisPublishOptions>, Slot>`.
That bound is the one place a handler body says which broker it runs on.

The step sets an option of the publish, and it wraps nothing.
So the record still leaves through the entry the mount site named: that entry's codec encodes it, its transforms run, and a test attributes the publish to its slot.
A test reads the options of a slot's publishes with `tb.out::<Slot>().with_options(..)`.

## Example

Every journal entry of one tenant carries the tenant as its key, so the journal keeps each tenant's order.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_publishing_step.rs#step"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_publishing_step.rs#step"
```

</TabItem>
</Tabs>
