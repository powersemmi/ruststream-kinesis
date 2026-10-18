---
title: LeaseStore
description: "A lease store holds each shard's owner, checkpoint and where its reading opened: acquire, renew, checkpoint, record_opened_at, read and release, with SHARD_END marking a finished shard."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# LeaseStore

A [`LeaseStore`](api:ruststream_kinesis::LeaseStore) holds each shard's owner, checkpoint and where its reading opened: acquire, renew, checkpoint, record_opened_at, read and release, with [`SHARD_END`](api:ruststream_kinesis::SHARD_END) marking a finished shard.

Kinesis keeps no record of who reads a shard or how far a reader got, so the subscription keeps both in a store.
[`KinesisBroker::lease_store`](api:ruststream_kinesis::KinesisBroker::lease_store) installs one, and every subscription of the broker uses it.
Without the call the broker uses a [`MemoryLeaseStore`](./memory-lease-store.md), correct for one instance of a service.
Several instances share the shards through a store they all reach, such as [`DynamoLeaseStore`](./dynamo-lease-store.md).
A subscription [acquires](api:ruststream_kinesis::LeaseStore::acquire) a shard for its instance before it reads it, and a shard another live instance holds is left to that instance.
A lease lasts ten seconds, and the reader [renews](api:ruststream_kinesis::LeaseStore::renew) it every three.
A failed renewal means another instance has taken the shard, and the reader stops at once without writing a checkpoint.
An expired lease is free to take, so the shards of an instance that stopped move to the others.
Every acknowledgement that moves a shard's [checkpoint](../settlement/index.md) is one [`checkpoint`](api:ruststream_kinesis::LeaseStore::checkpoint) call, accepted only while the instance holds the lease.
Before a reader hands out the first record it read, it [records](api:ruststream_kinesis::LeaseStore::record_opened_at) that record as the place its reading opened, once per reading.
A reader starts after the checkpoint the store [reads](api:ruststream_kinesis::LeaseStore::read) back as [`LeaseState`](api:ruststream_kinesis::LeaseState).
Without a checkpoint it starts at the record the shard's last reading opened at, so an instance that takes a shard over delivers again what the previous owner read and had not settled.
A shard that has neither opens at the subscription's start position, the tip unless `start_at` names another.
A shard read to its end is checkpointed as `SHARD_END`, which lets its children start, and then [released](api:ruststream_kinesis::LeaseStore::release).
A reader that stops also releases its shard, so the next owner need not wait for the lease to expire.
Each call names the shard by its [`LeaseKey`](./lease-key.md), so one store serves every stream of a broker.
A store that fails a call reports a [`LeaseError`](api:ruststream_kinesis::LeaseError); the subscription reports it as [`KinesisError::Lease`](api:ruststream_kinesis::KinesisError::Lease) naming the stream and the shard, and tries the shard again at its next listing.
The in-process transport of a [`TestApp`](api:ruststream::testing::TestApp) test consults no store.

## Example

The service takes its store from the code that starts it; two instances on one store split the shards of `inventory`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_leases_store.rs#store"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_leases_store.rs#store"
```

</TabItem>
</Tabs>
