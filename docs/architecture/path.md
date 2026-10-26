---
title: Path
sidebar_position: 4
description: "The way of a Kinesis record through a RustStream service: the read, the delivery, the handler, the reply and the checkpoint, one at a time or in batches."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of a record, one at a time with its reply on the left and in batches on the right, from the read to the checkpoint",
  "text": "The diagram as text",
  "single": "One at a time",
  "batches": "Batches",
  "receive": ["GetRecords", "up to 1000 records"],
  "channel": ["Channel", "64 deliveries, every shard"],
  "message": ["KinesisMessage", "headers, a place"],
  "handler": ["Handler", "decodes and runs"],
  "reply": ["Reply: PutRecord", "the resolved key"],
  "ack": ["ack", "checkpoint if contiguous"],
  "batchReceive": ["GetRecords", "limit n, up to 10,000"],
  "batchChannel": ["Channel", "64 deliveries"],
  "batchMessages": ["A batch", "n records, or 50 ms"],
  "batchHandler": ["Batch handler", "one call per batch"],
  "batchAck": ["Each element", "settles on its own"],
  "batch": "records"
};

# Path

The way of a Kinesis record through a RustStream service: the read, the delivery, the handler, the
reply and the checkpoint, one at a time or in batches.

<Diagram name="path" words={words} />

A shard's reader asks for up to 1,000 records per `GetRecords`. Each record becomes a
[`KinesisMessage`](api:ruststream_kinesis::KinesisMessage): the payload is copied out of the SDK's
answer and the [envelope](kinesis/index.md) is read off it when there is one. The partition key,
the sequence number and the shard id become headers. The delivery also takes its place in the
shard's watermark, which decides its checkpoint. These are a few allocations per record, and
[Benchmarks](/architecture/benchmarks/) publishes what a record costs.

Every reader of the subscription sends its deliveries into one channel of 64. A reader waits while
the channel is full, so the handlers set the pace of the reads. A read is stamped before it is
sent: if the subscription [repositions](kinesis/reading.md) while the read is in flight, its records
are dropped on their way out of the channel and are never handed to a handler.

A batch subscription makes its batch size the limit of every read, capped at the 10,000 a read can
return. The batch closes when it holds that many records, or 50 milliseconds after its first
record, so a batch is often smaller than the size. A batch may hold records of several shards.
Every element settles on its own, with its own checkpoint.

A reply is encoded into a buffer the dispatch loop reuses. The partition key comes from the publish
call, then from a `partition-key` header, then from the policy, and a record nobody keyed gets a key
that spreads records over the shards. User headers go into the envelope, and a reply without them is
the encoded buffer as it stands, unless it begins like an envelope. The [`KinesisPublisher`](api:ruststream_kinesis::KinesisPublisher)
writes it with one `PutRecord`, through the SDK client of the runtime that publishes.

The delivery settles after the reply. An `ack` writes a checkpoint only when it moves the shard's
watermark, which [Settlement](settlement.md) draws.
