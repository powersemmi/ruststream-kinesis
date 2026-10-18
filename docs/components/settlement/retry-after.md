---
sidebar_position: 1
title: retry_after
description: "retry_after publishes a copy of the record back to its stream once the delay is over, with the attempt count in a header; the copy arrives at the tip."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) publishes a copy of the record back to its stream once the delay is over, with the attempt count in a header; the copy arrives at the tip.

Kinesis holds no record back and counts no deliveries, so the runtime does both.
It settles the delivery, waits out the delay and publishes a copy of the record, payload and headers, to the stream the subscription reads.
The descriptor reports that stream as the address of its copies, so the registration needs no publisher of its own.
The copy carries [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) incremented, and the handler reads it like any other header.
The copy keeps the record's partition key, so it lands on the shard of that key.
It is a new record at the tip of that shard, so it loses its place among the records it was published with.
Every consumer of the stream reads the copy, since it is an ordinary record of the stream.
The copy waits in the service's memory, so it is at-most-once over the delay: a process that exits before the timer fires loses it.
[`max_attempts(n)`](./dead-letters.md) caps the deliveries of one record, and [`out_retry`](./out-retry.md) replaces the publisher the copies leave through.
Where the order of a key matters more than the delay, a plain [`retry`](./index.md) holds the shard's checkpoint instead.

## Example

An order that cannot be settled yet comes back thirty seconds later.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_retry.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_retry.rs#handler"
```

</TabItem>
</Tabs>
