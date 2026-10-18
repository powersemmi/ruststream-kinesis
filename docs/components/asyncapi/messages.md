---
sidebar_position: 3
title: Messages
description: "A policy with a partition key reports it on the message object, since a key routes one record and names no stream."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Messages

A policy with a partition key reports it on the message object, since a key routes one record and names no stream.

A key named on the policy with [`KinesisPublishSettings`](../publishing/publish-settings.md) applies to every record that leaves through it.
So the message object of those records carries it as `partitionKey` in its `x-ruststream-kinesis` object.
The stream stays on the [channel](./channels.md): one message type published to two streams describes both with one message object.
A policy with no key adds no object to the message.
A key named on one publish with the [`partition_key`](../publishing/partition-key.md) step describes one record, not the mount, so it is not in the document.

## Example

The receipts leave through a policy keyed `tenant-acme`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_asyncapi.rs#messages"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_asyncapi.rs#messages"
```

</TabItem>
</Tabs>

The message object of `Receipt` in the document:

```json reference="tests/snippets/asyncapi-message.json"
```
