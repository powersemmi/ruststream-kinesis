---
title: Lifecycle
sidebar_position: 3
description: "KinesisBroker::new records configuration, connect loads the AWS configuration, each subscription opens its shards, and shutdown closes the connection."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's states as types, and what each transition does on AWS",
  "text": "The diagram as text",
  "states": "Broker",
  "aws": "On AWS",
  "unconnected": ["KinesisBroker::new()"],
  "connected": ["ConnectedKinesis", "Broker"],
  "closed": ["Closed = ()", "nothing to report"],
  "config": ["Configuration only,", "no I/O"],
  "load": ["AWS configuration,", "the client, the lease", "store, the owner id"],
  "subscriptions": ["Each subscription:", "DescribeStreamSummary", "or CreateStream,", "ListShards, a lease", "and an iterator", "per shard"],
  "running": ["Shards listed every", "10 s, leases renewed", "every 3 s"],
  "flag": ["The connection", "is marked closed"],
  "afterShutdown": ["A publisher or a seek handle that outlives the connection", "answers KinesisError::NotConnected, never a silent success"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# Lifecycle

[`KinesisBroker::new`](api:ruststream_kinesis::KinesisBroker::new) records configuration, connect
loads the AWS configuration, each subscription opens its shards at startup, and shutdown closes the
connection.

<Diagram name="lifecycle" words={words} />

`new`, `from_config`, `endpoint`, `region`, `test_credentials`, `lease_store` and `owner_id` do no
I/O, so the service's builder stays synchronous. A clone of the broker shares its connection, and a
publisher taken from the broker before connect works once the runtime has connected it.

Connect loads the AWS configuration through the SDK's default chain, with the endpoint and region
the broker names. A broker built with `from_config` keeps its `SdkConfig`, except an endpoint or a region the
broker names. Then connect
builds the Kinesis client. Without `lease_store` the broker keeps its leases in a
[`MemoryLeaseStore`](api:ruststream_kinesis::MemoryLeaseStore), and without `owner_id` it takes a
lease-owner id that is unique to it. The SDK fetches credentials lazily, on the first request. In a
service that request is the first subscription's `DescribeStreamSummary`, so a bad credential fails
the start on subscribe.

Each subscription's [`KinesisStream`](api:ruststream_kinesis::KinesisStream) is checked first: an
empty name, or `create_if_missing` with no shards, is
[`KinesisError::Invalid`](api:ruststream_kinesis::KinesisError::Invalid) before any call to AWS. A
stream the broker already reads through another subscription is refused too. Then the subscription
asks for the stream with `DescribeStreamSummary`. A missing stream is
[`KinesisError::Stream`](api:ruststream_kinesis::KinesisError::Stream), unless `create_if_missing`
creates it and waits up to a minute for it.

The subscription returns once it reads. The coordinator's first pass lists the shards, takes the
lease of each shard it can, and starts a reader per shard. Every one of those readers fetches its
first iterator before `subscribe` returns. So the tip a shard opens at is the moment the
subscription opened, and a record published after that is read. An error of the first pass is
reported on the subscription's stream once it runs.

While the service runs, the coordinator lists the shards every 10 seconds and starts readers for
shards it can now take. Each reader renews its lease every 3 seconds. A reader stops when its
subscription is dropped, and it releases its lease so another instance can take the shard at once.

Shutdown marks the connection closed and returns `()`: the SDK client has nothing to close or
flush. A publisher or a seek handle created before shutdown answers
[`KinesisError::NotConnected`](api:ruststream_kinesis::KinesisError::NotConnected) afterwards.
