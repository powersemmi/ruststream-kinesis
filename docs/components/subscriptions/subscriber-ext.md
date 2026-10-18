---
sidebar_position: 4
title: KinesisSubscriberExt
description: "KinesisSubscriberExt puts poll_interval and create_if_missing on the mount chain, after batch(n) and before start_at(..)."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KinesisSubscriberExt

[`KinesisSubscriberExt`](api:ruststream_kinesis::KinesisSubscriberExt) puts `poll_interval` and `create_if_missing` on the mount chain, after `batch(n)` and before `start_at(..)`.

The two steps are the mount-site spellings of [`poll_interval`](./poll-interval.md) and [`create_if_missing`](./create-if-missing.md) on [`KinesisStream`](./kinesis-stream.md).
They suit a handler whose attribute names the stream and leaves the read settings to the code that mounts it.
They change the subscription's descriptor, so they need the descriptor to still be there.
The framework's own steps, such as `workers(n)` and `batch(n)`, come first.
[`start_at(..)`](../positions/start-at.md) comes last: it wraps the descriptor in a start position, and the Kinesis steps are not offered after it.
A handler that names its start position in the attribute names the read settings on the descriptor instead.
In the manual form the steps follow `.build()` on [`subscriber(source, body)`](api:ruststream::runtime::subscriber).

## Example

The archive reads the whole retained stream with four workers, a 250 ms pause and a stream it creates on a bare stand.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_subscriptions_ext.rs#chain"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_subscriptions_ext.rs#chain"
```

</TabItem>
</Tabs>
