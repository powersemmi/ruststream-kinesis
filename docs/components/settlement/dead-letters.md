---
sidebar_position: 2
title: Dead letters
description: "max_attempts caps deliveries per record; dead_letter names the stream a spent record leaves for, and a cap without one retires the record."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dead letters

[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) caps deliveries per record; [`dead_letter(stream)`](api:ruststream::runtime::Mounting::dead_letter) names the stream a spent record leaves for, and a cap without one retires the record.

The cap counts the first delivery too.
Kinesis keeps no delivery count, so the count travels with the record in [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER).
Under a cap every retry is a copy published back to the stream, the immediate [`retry`](./index.md) too, because only a copy carries the count forward.
When a retry would make the delivery the `n`-th one, the runtime settles it and publishes the record to the dead-letter stream instead, without the delay.
The record keeps its payload, its headers and its partition key; its retry count says how many times it was delivered.
A cap without a dead-letter stream checkpoints past the spent record, as [`drop`](api:ruststream::runtime::HandlerOutcome::drop) does.
The dead-letter stream is an ordinary stream of the service's own infrastructure: a subscription of the same service, or of another one, reads it.

## Example

An order gets four deliveries; the fourth one that still cannot be settled goes to `orders.unsettled`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_retry.rs#retry"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_retry.rs#retry"
```

</TabItem>
</Tabs>
