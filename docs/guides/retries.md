---
sidebar_position: 5
description: "Retry a Kinesis record now or later, cap the attempts and move what keeps failing to a dead-letter stream, and know what a copy does to order."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries and dead-letter streams

This guide retries a record now or later, caps the attempts and moves what keeps failing to a [dead-letter stream](../components/settlement/dead-letters.md).

Kinesis does not hold a record back, does not count deliveries and has no dead-letter queue.
The framework does all three: the mount site declares the cap and the dead-letter stream, and a retry travels as a copy of the record with a retry count in a header.

The example is a refunds service that sends refunds through a card processor.

## The subscriber

The processor refuses a refund in three ways, and each needs a different outcome.
`Busy` means its rate limit is used up: a retry right now would be refused again, so [`retry_after`](../components/settlement/retry-after.md) waits 30 seconds.
`Unavailable` means it did not answer: `retry` tries again at once.
`Declined` means the card account is closed, and another try gets the same answer, so the refund is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

[`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) settles the record and publishes a copy back to `refunds` once the delay has passed.
The copy carries the retry-count header ([`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)), raised by one.
The copy waits in the service's memory, so it is at-most-once: a process that stops before the delay ends loses it, and the original is already behind the checkpoint.

[`retry`](api:ruststream::runtime::HandlerOutcome::retry) under a declared cap also becomes a copy, published at once.
Without a cap it holds the shard's checkpoint in front of the record, as [Checkpoints and delivery guarantees](checkpoints.md) shows.

## The cap and the dead-letter stream

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) counts every delivery of one refund, the first included.
The fifth delivery is not retried: [`dead_letter`](api:ruststream::runtime::Mounting::dead_letter) publishes it to `refunds.review`, payload and headers as they arrived.
A cap without a dead-letter stream checkpoints past the spent record instead, as `drop` does.
A dropped refund settles and goes nowhere; the dead-letter stream holds only the refunds that ran out of attempts.
The streams belong to the service's infrastructure; the example creates them with `create_if_missing` for a local stack.

## Reading the dead-letter stream

The dead-letter stream is an ordinary stream, and `review` reads it like any other.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

The retry-count header of a dead-lettered record holds the number of deliveries it went through, 5 here.
`review` reads it with `ctx.headers()` and logs the refund for a person to look at.

## Copies and order

A copy goes back to the stream the subscription reads, so a registration on Kinesis needs no retry destination of its own.
It keeps the record's partition key, so it lands on the shard of that key.
It lands at the tip of that shard, though, after the records published while it waited.
A refund that waits 30 seconds is therefore handled after the refunds of the same card that came later.

Where the order of a key matters more than moving on, declare no cap and let `retry` hold the shard: the checkpoint stops at the record, and the shard is read again from it when its lease is next taken.
[`out_retry`](../components/settlement/out-retry.md) replaces the publisher the copies leave through, so a transform can stamp each copy on its way out.

## Testing the delay

The tests in the example file run the app in process with a paused clock, so `tb.advance(BACKOFF)` lets the 30 seconds pass at once.
`TestApp::with_state` starts the app with a stand-in processor that is busy or silent.
[Testing a Kinesis service](testing.md) covers the harness.
