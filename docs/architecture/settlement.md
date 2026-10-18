---
title: Settlement
sidebar_position: 6
description: "What ack, drop, retry and retry_after become on Kinesis, which keeps one checkpoint per shard instead of acknowledging records."
---

import {Diagram} from '@site/src/components/Diagrams';

export const watermark = {
  "title": "The watermark on one shard: the checkpoint stops before the first delivery that has not settled",
  "text": "The diagram as text",
  "shard": "One shard, records in the order they arrived",
  "settled": "settled",
  "unsettled": "running",
  "checkpoint": ["Checkpoint: record 2", "the last record before", "the first running one"],
  "store": ["LeaseStore", "::checkpoint", "if the lease is held"],
  "next": ["When 3 settles,", "the checkpoint moves", "to 5: 6 is still", "running"],
  "at": "names",
  "writes": "writes"
};

export const retries = {
  "title": "A retry on Kinesis: the attempt cap decides between a copy to the stream, the dead-letter stream and a checkpoint past the record",
  "text": "The diagram as text",
  "record": ["A record", "x-ruststream-retry-count says which attempt it is"],
  "outcome": ["The handler returns", "retry_after(delay), or retry() under a declaration"],
  "cap": ["max_attempts: is this attempt below the cap?"],
  "copy": ["Below the cap:", "the original", "settles, a copy", "goes to the stream"],
  "deadLetter": ["At the cap:", "the record goes", "to the dead-", "letter stream"],
  "past": ["At the cap,", "no dead letter:", "the checkpoint", "passes it"],
  "tip": ["The copy is a new record at the tip,", "on the shard its key picks"],
  "crash": ["The delayed copy is held in the process:", "a crash before the timer fires loses it"],
  "handled": "handled",
  "retries": "a retry",
  "below": "below",
  "atCap": "at the cap"
};

# Settlement

What `ack`, `drop`, `retry` and `retry_after` become on Kinesis, which keeps one checkpoint per
shard instead of acknowledging records.

<Diagram name="watermark" words={watermark} />

A checkpoint names one record of a shard and says that every record before it is handled. So the
crate tracks the deliveries of each shard in the order they arrived. A settlement marks its
delivery handled, and the checkpoint moves to the last record of the run of handled deliveries
that starts at the oldest one. Acks from parallel lanes can arrive in any order, and the checkpoint
never passes a record that is still running. A settlement writes to the lease store only when it
moves the checkpoint, and the store takes the write only while this instance holds the shard.

| Outcome | Effect on the shard |
|---|---|
| `ack` | Handled: the checkpoint moves when no earlier record is still running |
| `drop` | Handled, as for `ack`: the checkpoint passes a poison record |
| `retry()` | Refused (`AckError::Unsupported`) and logged: the record stays unhandled and holds the checkpoint, and the shard is read again from it when its lease is next taken |
| `retry()` under `max_attempts` or a dead-letter stream | A counted copy goes to the stream, and the original is handled |
| `retry_after(delay)` | A counted copy goes to the stream once the delay is over, and the original is handled |
| The cap is spent | The record goes to the dead-letter stream, or without one it is handled and the checkpoint passes it |
| No settlement before shutdown | The checkpoint stays where it is, and the shard's next owner reads the record again |

A shard is read in order, and Kinesis cannot hand one record back, so a requeue is refused. The
reader goes on, and the record comes back after a restart or a lease handover, together with every
record after it. Delivery is at least once.

A [seek](kinesis/reading.md) starts the watermark of the shards it moves afresh. A settlement of a
delivery read before the seek changes nothing, so a late ack never moves a checkpoint back to where
the seek came from. A closed shard is marked `SHARD_END` only after every one of its deliveries has
settled, and its children start after that.

## Retries

<Diagram name="retries" words={retries} />

Kinesis holds no record back and counts no deliveries, so the runtime does both. A `retry_after`
outcome, and a `retry()` on a registration that declares
[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) or a
[`dead_letter`](api:ruststream::runtime::Mounting::dead_letter) stream, take this path. The
attempt count travels in the copy's `x-ruststream-retry-count` header, and the first delivery
counts as one.

The copy goes to the stream the subscription reads, the address the
[`KinesisStream`](api:ruststream_kinesis::KinesisStream) descriptor reports, so a registration
names no destination. It lands at the tip, on the shard its partition key picks, behind the records
written since. So a retried record loses its order against its neighbours. Where that order
matters, declare no cap and let `retry()` hold the shard. Every consumer of the stream receives the
copy, not only the one that retried.

A spent record goes to the dead-letter stream with its payload and headers. A cap without a
dead-letter stream retires the record the way `drop` does.
