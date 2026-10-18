---
sidebar_position: 2
title: Channels
description: "A subscription's channel reports its stream, read pause and provisioned shards; a publishing position's channel reports the stream it writes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Channels

A subscription's channel reports its stream, read pause and provisioned shards; a publishing position's channel reports the stream it writes.

A [`KinesisStream`](../subscriptions/kinesis-stream.md) writes three fields into its channel's `x-ruststream-kinesis` object.
`stream` is the stream it reads, and `pollIntervalMs` the [pause between reads](../subscriptions/poll-interval.md) of an idle shard, in milliseconds.
`shardCount` is the number of shards [`create_if_missing`](../subscriptions/create-if-missing.md) gives the stream, and it is there only when the descriptor creates the stream.

A channel the service publishes to reports `stream`, the stream its records land in.
The [policy](../publishing/index.md) holds no destination, so the stream is the one the mount site resolved: a reply type's `#[outgoing(name)]`, a `reply("stream")` clause, or the name of a slot entry.

## Example

The subscription reads `orders` every quarter of a second and creates it with two shards, and its replies land in `receipts`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_asyncapi.rs#channels"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_asyncapi.rs#channels"
```

</TabItem>
</Tabs>

The channel `orders` in the document:

```json reference="tests/snippets/asyncapi-channel.json"
```

The channel `receipts`:

```json reference="tests/snippets/asyncapi-publish-channel.json"
```
