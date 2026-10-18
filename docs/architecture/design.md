---
title: Design
sidebar_position: 2
description: "Where ruststream-kinesis sits: between the RustStream runtime and the AWS SDK, a reader per shard, a lease store beside them, one SDK client per runtime."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the AWS SDK clients, and Kinesis and DynamoDB, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": "Runtime: dispatch, lanes, settlement",
  "crate": "ruststream-kinesis",
  "subscriber": ["KinesisSubscriber", "a reader per shard"],
  "message": ["KinesisMessage", "ack = checkpoint"],
  "publisher": ["KinesisPublisher", "one PutRecord"],
  "descriptor": ["KinesisStream", "descriptor"],
  "broker": ["KinesisBroker", "configuration"],
  "policy": ["KinesisPublish", "policy"],
  "leases": ["LeaseStore", "leases, checkpoints"],
  "sdk": "AWS SDK for Rust",
  "kinesisClient": ["aws-sdk-kinesis", "a client per runtime"],
  "dynamoClient": ["aws-sdk-dynamodb", "with dynamodb-lease"],
  "kinesis": ["Kinesis", "Data Streams"],
  "dynamodb": ["DynamoDB", "lease table"],
  "inProcess": ["Under the testing feature, an in-process log", "takes the place of the client and of Kinesis"],
  "mounted": "mounted",
  "polls": "polls the stream",
  "https": "HTTPS"
};

# Design

Where `ruststream-kinesis` sits: between the RustStream runtime and the AWS SDK, a reader per shard,
a lease store beside them, one SDK client per runtime.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over the
[AWS SDK for Rust](https://docs.rs/aws-sdk-kinesis/latest/aws_sdk_kinesis/). The runtime above it
is the same for every broker. Everything Kinesis-specific is in this crate: the
[`KinesisStream`](api:ruststream_kinesis::KinesisStream) descriptor, the shard readers, the leases,
what an outcome does to a checkpoint, and how a record carries headers. The crate subscribes, takes
batches, repositions a subscription, hands the partition key to keyed dispatch, and describes its
server in the generated document. It has no transactions and no request-reply.

Each subscription is one stream. A coordinator task lists the stream's shards, takes a lease for
each shard it can, and starts a reader task per shard it holds. Every reader feeds one channel of
64 deliveries, which the subscription's stream drains. A batch subscription groups the same
deliveries on the client. The coordinator and the readers run on the runtime that connected the
broker.

The connected broker and every publisher share one connection: the SDK client, the lease store,
the lease-owner id and a closed flag. The SDK opens an HTTP connection on the runtime of the request
that needs it. So each runtime sends through a client of its own: the broker's runtime uses the one
built at connect, and a handler thread with a runtime of its own builds one on its first request,
from the same configuration and credential cache. A busy thread then never stalls the broker's
connections.

A [`KinesisPublish`](api:ruststream_kinesis::KinesisPublish) policy pairs with the connection into
a [`KinesisPublisher`](api:ruststream_kinesis::KinesisPublisher), which writes each record with one
`PutRecord`. It takes the buffer the reply was encoded into, and a record without headers is that
buffer as it stands. A retry copy goes back to the stream the subscription reads, as
[Settlement](settlement.md) draws.

## In-process transport

Under the `testing` feature, a test connects the broker in process instead. The connected broker,
its subscriptions, its publishers and its deliveries then work against a log that lives in the
test. A build without the feature carries no trace of it: the compiler checks that the connection,
the delivery stream, the seek handle and the settlement are exactly the size of their live forms.

The log writes a record the way the publisher writes it for Kinesis, with the same partition key
and the same envelope, and reads it back with the same decoding. It refuses what Kinesis refuses
with the error a live call reports: a bad stream name, a bad partition key, a record that is too
large, and any handle used after shutdown. It models positions and seeking, the delivery headers,
retry copies and the dead-letter stream, and answers a requeue with `AckError::Unsupported`.

Some things belong to the live service only. A stream has one shard in process, so every record is
ordered against every other. There are no leases, no checkpoints, no retention and no resharding,
and a record left unhandled does not come back. Every stream a service names exists, and a
subscription opens at the tip.

How the pieces start and stop is on [Lifecycle](lifecycle.md), and the way of one record on
[Path](path.md).
