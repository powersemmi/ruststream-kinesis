---
sidebar_position: 5
title: LeaseKey
description: "LeaseKey names one shard of one stream; a store of your own keys its rows by the key's stream:shard form."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# LeaseKey

[`LeaseKey`](api:ruststream_kinesis::LeaseKey) names one shard of one stream; a store of your own keys its rows by the key's `stream:shard` form.

Every [`LeaseStore`](./index.md) call names its shard by a `LeaseKey`.
The stream is the name the subscription's [`KinesisStream`](../subscriptions/kinesis-stream.md) was given, and the shard is the id the service assigned.
Shard ids repeat across streams, so a store keys by both, and one store serves every stream of a broker.
`LeaseKey` displays as `stream:shard`; a stream name holds no colon, so the form is unambiguous, and a store that keeps one string key writes it.
A store of your own implements the five methods of `LeaseStore` and is installed with [`KinesisBroker::lease_store`](api:ruststream_kinesis::KinesisBroker::lease_store).
A store for one instance grants every lease; a store for several instances makes `acquire`, `renew` and `checkpoint` conditional on the owner, as [`DynamoLeaseStore`](./dynamo-lease-store.md) does.

## Example

A store for one instance keeps its checkpoints in a map keyed by the key's string form.

{/* single-form: a lease store is a trait implementation, the same with the macros and without them */}
```rust reference="examples/kinesis_leases_key.rs#key"
```

The service installs it on its broker.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_leases_key.rs#mount"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_leases_key.rs#mount"
```

</TabItem>
</Tabs>
