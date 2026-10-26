---
title: Streams
sidebar_position: 0
description: "A Kinesis stream is a set of shards, each an ordered log of records, and every consumer reads every shard for itself until retention ends."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Producers put records into the three shards of one stream, and two consumers each read every shard",
  "text": "The diagram as text",
  "producers": ["Producers", "the partition key picks the shard"],
  "stream": "Stream orders",
  "s0": "Shard 0",
  "s1": "Shard 1",
  "s2": "Shard 2",
  "first": ["Consumer billing", "an iterator per shard,", "a checkpoint per shard"],
  "second": ["Consumer audit", "an iterator per shard,", "a checkpoint per shard"],
  "retention": ["A record stays for every reader until retention ends:", "24 hours by default, up to 365 days"],
  "puts": "PutRecord",
  "reads": "reads every shard"
};

# Streams

A Kinesis stream is a set of shards, each an ordered log of records, and every consumer reads every
shard for itself until retention ends.

<Diagram name="stream" words={words} />

A producer writes a record with `PutRecord`: the stream's name, a partition key and a data blob.
Kinesis hashes the key with MD5 into a 128-bit number. Each shard owns a range of those numbers,
so the key picks the shard, and the records of one key stay in order. Order holds inside a shard
and nowhere else.

A record holds a data blob and a partition key, and nothing else: there are no headers. The key is
1 to 256 Unicode characters, and the blob with its key is at most 10 MiB. A stream is named by 1 to
128 characters of letters, digits, `_`, `.` and `-`. This crate names a stream by its name.

Kinesis numbers each record with a sequence number when the record arrives. Within a shard the
numbers grow over time. The crate keeps track of the order in which a shard delivered its records
and never compares the numbers themselves.

A record stays in the stream until its retention ends, whether anyone read it or not: 24 hours by
default, up to 365 days. Every consumer reads every shard on an iterator of its own, so two
consumers each see every record. What a consumer has read is one position per shard, its
checkpoint. Kinesis stores no checkpoints and no acknowledgements: the consumer keeps them, in a
[lease table](leases.md).

User headers have no place in a record, so the crate writes them into a small envelope in front of
the payload, and only when there are any. A record published without headers is the plain payload,
which any consumer reads, unless the payload itself begins like an envelope: then it gets one with
no headers, so it reads back whole.

The model is Amazon Kinesis Data Streams' own
([key concepts](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html),
[`PutRecord`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html),
[retention](https://docs.aws.amazon.com/streams/latest/dev/kinesis-extended-retention.html)). How
shards split and merge is on [Shards and resharding](shards.md), and how a consumer reads one on
[Reading](reading.md).
