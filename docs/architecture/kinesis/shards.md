---
title: Shards and resharding
sidebar_position: 1
description: "A shard carries a fixed share of a stream's throughput; a split or a merge closes shards and opens children, which are read after their parents."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A split turns shard 0 into shards 1 and 2, a merge turns shards 1 and 2 into shard 3, and each child is read after its parents",
  "text": "The diagram as text",
  "s0": ["Shard 0, closed", "an ending sequence number"],
  "s1": ["Shard 1, closed", "the lower half of 0"],
  "s2": ["Shard 2, closed", "the upper half of 0"],
  "s3": ["Shard 3, open", "parents 1 and 2"],
  "gate": ["A child is read only once each parent is read to its end,", "so the records of one key keep their order", "across the change"],
  "split": "split",
  "merge": "merge"
};

# Shards and resharding

A shard carries a fixed share of a stream's throughput; a split or a merge closes shards and opens
children, which are read after their parents.

<Diagram name="resharding" words={words} />

Each shard takes up to 1 MB or 1,000 records a second in writes. It gives up to 2 MB a second in
reads, and at most five `GetRecords` calls a second. Every consumer that reads the shard shares
those five calls, so the limit is not billing: it is how often a shard can be read at all. A
stream in provisioned mode has the shards it was given, and one in on-demand mode gets as many as
its traffic needs.

A split divides one shard's hash key range between two children. A merge joins two adjacent shards
into one. Either way the parents are closed: `ListShards` reports an ending sequence number for
them, they take no new records, and they keep the ones they have until retention ends. A reader
that reaches the end of a closed shard gets no next iterator: that is `SHARD_END`.

The records of one key may sit on a parent and then on a child. To keep their order, a consumer
reads each parent to its end before it reads the child. The crate checks this for each child
before it takes the child's lease: every parent must be marked `SHARD_END` in the lease store, or
gone from the listing because retention trimmed it. A closed parent with no checkpoint counts as
read when the subscription starts at the tip, since its old records are skipped anyway. A reader
writes `SHARD_END` only after every delivery of its shard has settled.

The subscription lists the shards again every 10 seconds, so a child opens within that time once
its parents are done. A child's reader resumes from the child's checkpoint, as
[Reading](reading.md) draws. `create_if_missing(shards)` creates a
missing stream in provisioned mode with that many shards and waits up to a minute for it to become
usable.

The model is Amazon Kinesis Data Streams' own
([quotas and limits](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html),
[capacity modes](https://docs.aws.amazon.com/streams/latest/dev/how-do-i-size-a-stream.html),
[resharding](https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-resharding.html),
[reading after resharding](https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-after-resharding.html)).
