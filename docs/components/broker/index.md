---
title: KinesisBroker
description: "KinesisBroker connects a service to Kinesis Data Streams; new records configuration only, and the runtime builds the SDK client at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# KinesisBroker

[`KinesisBroker`](api:ruststream_kinesis::KinesisBroker) connects a service to Kinesis Data Streams; [`new`](api:ruststream_kinesis::KinesisBroker::new) records configuration only, and the runtime builds the SDK client at startup.

`KinesisBroker::new()` does no I/O, so the whole service fits the synchronous `#[ruststream::app]` builder.
The settings that follow it are builder steps too: the AWS [credentials and region](./credentials.md), a local [endpoint](./endpoint.md), and the [lease store](../leases/index.md) and [owner id](../leases/owner-id.md) that let several instances share the shards.
A setting the service leaves out comes from the AWS SDK's default chain.

At startup the runtime connects the broker and receives a [`ConnectedKinesisBroker`](api:ruststream_kinesis::ConnectedKinesisBroker).
The connect loads the AWS configuration and builds the SDK client; it sends no request to Kinesis.
Every subscription and every publisher of the service runs on that connected form, and the first requests go out when the subscriptions [start](./startup.md).
A handler that runs on a thread of its own publishes and checkpoints through a client built for that thread from the same configuration, so one busy runtime never holds up another's requests.
At [shutdown](./shutdown.md) the runtime consumes the connected form, and the connection is closed.

`#[subscriber("orders")]` reads every shard of the stream `orders`.
The descriptor [`KinesisStream`](../subscriptions/kinesis-stream.md) carries the settings of a subscription, and the [Subscriptions](../subscriptions/index.md) pages describe them.
Records leave through the [`KinesisPublish`](../publishing/index.md) policy, the broker's default.

## Example

The service reads the stream `orders`.
The region and the credentials come from the environment it runs in.
`cargo run --example kinesis_service --features testing -- run` starts it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_service.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_service.rs#app"
```

</TabItem>
</Tabs>
