---
sidebar_position: 0
description: "The parts of ruststream-kinesis a Kinesis service uses, one page per component."
---

# Components

The parts of `ruststream-kinesis` a Kinesis service uses, one page per component: the broker
([`KinesisBroker`](api:ruststream_kinesis::KinesisBroker)), its stream subscriptions, its
checkpoints and lease stores, its positions, its publish policy and the in-process transport a
[`TestApp`](api:ruststream::testing::TestApp) test runs on.

The crate reads and writes Amazon Kinesis Data Streams through the official `aws-sdk-kinesis`
client.
On top of the client it finds a stream's shards, leases each shard to one instance of the service
and keeps a checkpoint per shard.
A service adds `ruststream-kinesis = "0.7"` to its dependencies, beside the framework itself.

Three cargo features are off by default and only add to the crate.
`dynamodb-lease` adds [`DynamoLeaseStore`](./leases/dynamo-lease-store.md), so several instances
of a service share the shards.
`asyncapi` adds the [Kinesis objects](./asyncapi/index.md) of the generated AsyncAPI document.
`testing` adds the [in-process mode](./testing/index.md) a `TestApp` test connects.
