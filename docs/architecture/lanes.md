---
title: Lanes
sidebar_position: 5
description: "One subscription reads its shards through a reader each and one channel; workers(n, by_key) spreads the records over lanes keyed by partition key."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One subscription's dispatch: three shards, a reader each, one channel and two lanes keyed by partition key",
  "text": "The diagram as text",
  "shard0": "Shard 0",
  "shard1": "Shard 1",
  "shard2": "Shard 2",
  "reader0": ["Reader", "lease held"],
  "reader1": ["Reader", "lease held"],
  "reader2": ["Reader", "lease held"],
  "channel": ["One channel of 64 deliveries", "a full channel holds every reader back"],
  "keyed": ["workers(2, by_key)", "lane = hash of the partition key"],
  "lane0": ["Lane 1", "keys alice and carol,", "each in order"],
  "lane1": ["Lane 2", "key bob, in order"],
  "single": ["Without workers, one loop runs the handler", "for one record at a time, in arrival order"],
  "instances": ["Instances that share a lease table share the shards:", "each shard is read by one instance at a time"],
  "oneAtATime": "one at a time"
};

# Lanes

One subscription reads its shards through a reader each and one channel; `workers(n, by_key)`
spreads the records over lanes keyed by partition key.

<Diagram name="lanes" words={words} />

A subscription's stream yields one record at a time, whichever shard it came from. Without
`workers`, one loop runs the handler for each record in turn, so every shard keeps its order and
nothing runs side by side. `workers(n)` runs up to `n` handlers at once, in no particular order.

`workers(n, by_key)` runs `n` lanes, and a record goes to the lane its key hashes to. The key is the
record's [`PARTITION_KEY_HEADER`](api:ruststream_kinesis::PARTITION_KEY_HEADER), which every
delivery fills from the record's own partition key. So the records of one key never spread over
two lanes, and the order Kinesis keeps for a key on its shard is kept in the service. Lanes fewer
than keys serve several keys each.

All readers of a subscription send into one channel of 64 deliveries. When the handlers fall
behind, the channel fills, every reader waits, and no shard is read ahead of what the service can
handle. Acks from parallel lanes arrive out of order, and the checkpoint still never passes a
record that has not settled, as [Settlement](settlement.md) draws.

Instances of a service that share a [lease table](kinesis/leases.md) share the shards: each shard
is read by one instance at a time, and a new instance takes shards as their leases come free. With
the default [`MemoryLeaseStore`](api:ruststream_kinesis::MemoryLeaseStore) every instance keeps its
own leases, so each one reads every shard.

A [batch handler](path.md) takes up to the size the mount site names, or what arrived within
50 milliseconds of the batch's first record, so a batch is often smaller than that size.
