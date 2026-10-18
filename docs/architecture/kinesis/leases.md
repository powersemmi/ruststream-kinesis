---
title: Leases
sidebar_position: 3
description: "Instances of a service share a stream's shards through a lease table: one row per shard with its owner, its expiry and its checkpoint."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Two instances share the shards of one stream through a lease table, and one takes over the other's shard when its lease lapses",
  "text": "The diagram as text",
  "a": ["Instance A", "holds shard 0"],
  "table": "Lease table",
  "row0": ["orders:shardId-000000000000", "owner A, expiry, counter, checkpoint"],
  "row1": ["orders:shardId-000000000001", "owner B, expiry, counter, checkpoint"],
  "b": ["Instance B", "holds shard 1"],
  "takeover": ["When A stops, its lease lapses after 10 s. B takes the row", "with a conditional write and resumes after", "the checkpoint A wrote"],
  "renews": "renews every 3 s"
};

# Leases

Instances of a service share a stream's shards through a lease table: one row per shard with its
owner, its expiry and its checkpoint.

<Diagram name="leases" words={words} />

Kinesis keeps no record of who reads a shard or how far. The Kinesis Client Library keeps it in a
DynamoDB table, one row per shard, and its workers take the rows that nobody renews. This crate
does the same through a [`LeaseStore`](api:ruststream_kinesis::LeaseStore). A store takes a shard
for an owner, renews it, records a checkpoint, reads a row and releases it. Every row is keyed by
the stream and the shard together, because shard ids repeat across streams.

A lease lasts 10 seconds, and its reader renews it every 3 seconds. A reader whose renewal fails
has lost the shard to another owner, and it stops at once without writing a checkpoint. The new
owner reads from the last checkpoint, so a few records may be delivered twice. Delivery is at least
once.

[`DynamoLeaseStore`](api:ruststream_kinesis::DynamoLeaseStore), under the `dynamodb-lease` feature,
keeps the rows in a DynamoDB table with one string key, `lease_key`, written as `stream:shard`.
A row holds `lease_owner`, `lease_expiry`, `lease_counter` and `checkpoint`. Every write is
conditional. A take succeeds when the row has no owner, has this owner, or has expired. A renewal,
a checkpoint and a release succeed only while this owner holds the row. Reads are strongly
consistent. Each broker has a lease-owner id of its own, unique by default, so two instances never
hold one shard together.

[`MemoryLeaseStore`](api:ruststream_kinesis::MemoryLeaseStore) is the default. It lives in the
process: correct for one instance, and empty again after a restart. A restarted service then has no
checkpoints, and each shard opens at the tip unless `start_at(..)` names another position.

A store belongs to one broker. One broker reads a stream through one subscription, and a second
subscription to the same stream is refused at startup. Two brokers with lease stores of their own,
in one service or in two, each read the whole stream. Instances that share one table share the
shards.

The model is the Kinesis Client Library's lease table
([KCL consumers](https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html)),
with DynamoDB's conditional writes
([condition expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html)).
