---
sidebar_position: 3
title: out_retry
description: "out_retry replaces the publisher the retry copies leave through, so the copies take a transform or a codec of the mount site's choosing."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# out_retry

[`out_retry(policy)`](api:ruststream::runtime::Mounting::out_retry) replaces the publisher the retry copies leave through, so the copies take a transform or a codec of the mount site's choosing.

A Kinesis registration publishes its [retry copies](./retry-after.md) without it, through the broker's default [`KinesisPublish`](../publishing/index.md) policy.
`out_retry` names the policy for that position, and the chain after it takes the position's steps: `.transform(..)` and `.codec(..)`.
A transform on the retry position reads the delivery it answers, through a [`PublishContext`](api:ruststream::runtime::PublishContext), and runs on every copy.
The copy carries the record's own bytes, so a codec named there encodes nothing.
The copy keeps the record's partition key and lands on that key's shard.

## Example

Every invoice copy says which stream it was deferred from.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_settlement_out_retry.rs#out_retry"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_settlement_out_retry.rs#out_retry"
```

</TabItem>
</Tabs>
