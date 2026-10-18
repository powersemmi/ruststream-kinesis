---
sidebar_position: 4
title: Shutdown
description: "Shutdown closes the connection; a publisher or a seek handle taken earlier reports KinesisError::NotConnected instead of acting."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

Shutdown closes the connection; a publisher or a seek handle taken earlier reports [`KinesisError::NotConnected`](api:ruststream_kinesis::KinesisError::NotConnected) instead of acting.

At shutdown the runtime stops the subscriptions first.
Each shard reader then releases its lease, so another instance can take the shard at once instead of waiting for the lease to expire.
A publish is one `PutRecord` call that returns when the service has stored the record, so the broker holds no queue to flush.
Then the runtime shuts the broker down.

A publisher taken before the shutdown keeps pointing at the closed connection.
[`KinesisBroker::publisher`](api:ruststream_kinesis::KinesisBroker::publisher) hands one out before the service starts, for code that publishes outside any handler.
It shares the broker's connection, so it publishes once the service is up.
After the shutdown a publish through it returns `KinesisError::NotConnected`, and never writes to a stream the service no longer reads.
A [seek handle](../positions/seek.md) kept past the shutdown answers the same way.

## Example

An admin endpoint beside the service publishes cancellations through a publisher taken from the broker.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_broker_shutdown.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_broker_shutdown.rs#service"
```

</TabItem>
</Tabs>
