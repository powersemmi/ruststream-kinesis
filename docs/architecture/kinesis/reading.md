---
title: Reading
sidebar_position: 2
description: "A consumer reads a shard with an iterator: GetShardIterator places it on the shard's log, and every GetRecords returns records and the next iterator."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The five iterator types placed on a shard's log, and the loop of GetRecords calls that reads from there",
  "text": "The diagram as text",
  "shard": "One shard, records in arrival order",
  "next": "next",
  "horizon": ["TRIM_", "HORIZON"],
  "timestamp": ["AT_", "TIMESTAMP"],
  "atSequence": ["AT_", "SEQUENCE_", "NUMBER"],
  "afterSequence": ["AFTER_", "SEQUENCE_", "NUMBER"],
  "latest": ["LATEST"],
  "getIterator": ["GetShardIterator", "shard, iterator type, position"],
  "getRecords": ["GetRecords", "iterator, limit 1000 or the batch size"],
  "answer": ["Records and NextShardIterator:", "the iterator of the next read;", "none at the end of a closed shard"],
  "expiry": ["An iterator expires 5 minutes after it was issued.", "An empty read waits poll_interval, 1 s by default"],
  "type": "an iterator type",
  "iterator": "an iterator"
};

# Reading

A consumer reads a shard with an iterator: `GetShardIterator` places it on the shard's log, and
every `GetRecords` returns records and the next iterator.

<Diagram name="iterator" words={words} />

The iterator type says where the first read starts. `TRIM_HORIZON` starts at the oldest record the
shard still holds. `LATEST` starts after the newest one, so the reader sees only what arrives next.
`AT_TIMESTAMP` starts at the first record that arrived at or after a time, by its arrival
timestamp. `AT_SEQUENCE_NUMBER` starts at one record, and `AFTER_SEQUENCE_NUMBER` right after it.

Each [`KinesisPosition`](api:ruststream_kinesis::KinesisPosition) is one of those types:
`horizon()`, `latest()`, `timestamp(ms)` and `sequence(shard, seq)`, which names one record of one
shard. A stored checkpoint opens the shard after the record it names. A shard with no checkpoint
opens at `LATEST`, unless `start_at(..)` names another position. `start_at(..)` applies at
startup, to the shards the subscription opens with, ahead of their checkpoints. A reader started
later, for a child shard or for a shard taken over from another instance, resumes from the
checkpoint; a child shard with no checkpoint opens at `TRIM_HORIZON`.

A `GetRecords` call returns up to 10,000 records or 10 MB. The crate asks for 1,000, or for the
batch size of a batch subscription. Every answer carries the next iterator, and the reader uses it
for its next call. An answer with no records makes the reader wait the descriptor's
[`poll_interval`](api:ruststream_kinesis::KinesisStream::poll_interval), one second by default,
before it reads again. That keeps the reader inside the five reads a second its shard allows.

An iterator expires five minutes after it was issued. A reader whose iterator expired fetches a new
one from its checkpoint, or, before anything is settled, from the first record it read; never from
`LATEST`, so no record is skipped. A throttled read waits one poll interval and tries again with
the same iterator. Any other failed read is reported on the subscription's stream, and the reader
waits a second and retries its iterator, which is still good. After eleven failures in a row the
reader stops and releases its lease; the subscription takes the shard again when it next lists the
shards, and the new reader starts where the old one did.

The crate reads with the stream's shared throughput, the five reads a second every consumer of a
shard shares. Enhanced fan-out, a dedicated 2 MB a second per consumer over `SubscribeToShard`, is
another read model. A record that a producer aggregated with the Kinesis Producer Library is
reported as [`KinesisError::AggregatedRecord`](api:ruststream_kinesis::KinesisError::AggregatedRecord)
and is not handed to a handler.

The model is Amazon Kinesis Data Streams' own
([`GetShardIterator`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html),
[`GetRecords`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html),
[enhanced fan-out](https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html),
[aggregation](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md)).
