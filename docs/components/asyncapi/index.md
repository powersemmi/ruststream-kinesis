---
title: Kinesis bindings
description: "With asyncapi the document speaks Kinesis: the server's host, and an x-ruststream-kinesis object on each channel and keyed message."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Kinesis bindings

With the `asyncapi` feature the generated document speaks Kinesis: the server's host, and an `x-ruststream-kinesis` object on each channel and keyed message.

The core builds the AsyncAPI document from the app, before anything connects.
The specification defines no Kinesis binding, and its list of binding keys is closed.
So this crate writes its own objects under the extension key `x-ruststream-kinesis`.
A [server](./server.md) reports the host clients connect to.
A [channel](./channels.md) reports the stream behind it, and a subscription's channel its read settings too.
A [message](./messages.md) reports the partition key the mount site fixed.

Everything comes from the service's own declarations: the broker, the descriptors and the policies.
The document is built without a connection, so an existing stream's real shard count is not in it.
The document is published and shared, so it carries no credential.
The binary `#[ruststream::app]` writes prints the document with `asyncapi gen`.

## Example

The dependency line:

```toml
ruststream-kinesis = { version = "0.7", features = ["asyncapi"] }
```

The service registers its broker under the label `kinesis`, the key of its server in the document.
`cargo run --example kinesis_asyncapi --features asyncapi -- asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_asyncapi.rs#app"
```

</TabItem>
</Tabs>
