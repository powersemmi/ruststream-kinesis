---
sidebar_position: 8
description: "Test a Kinesis service in process with TestApp: the production app, keyed records, replies, delays, and the same test against a local stack."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a Kinesis service

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp), against an in-process Kinesis or against a stack.

The service under test charges card payments.
A payment arrives on `card-payments`, and its partition key is the account to charge.
The handler replies with a receipt on `card-receipts`.
A payment without an amount is dropped.
A payment over the bank's instant limit is held: the handler asks for it again in a minute with [`retry_after`](../components/settlement/retry-after.md).
After three deliveries a payment goes to `card-payments.review`, the [dead-letter stream](../components/settlement/dead-letters.md).
The app is built by `service(broker)`, so a test chooses the broker and `main` uses the production one.
The tests need the `testing` feature of `ruststream-kinesis` in `[dev-dependencies]`.

## A keyed payment

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`KinesisBroker`](api:ruststream_kinesis::KinesisBroker) to an in-process Kinesis.
No AWS endpoint is called, and no credentials are needed.
The test addresses the broker by its production type, `tb.broker::<KinesisBroker>()`.
The partition key of a record travels as the `partition-key` header ([`PARTITION_KEY_HEADER`](api:ruststream_kinesis::PARTITION_KEY_HEADER)), so the test sets it with `with_headers`.
The broker turns that header into the record's partition key, and the delivery reports it back under the same header.
`publish` returns once the handler and its reply have settled.
So the next lines assert on finished state: how the subscriber settled and what reached `card-receipts`.
The body takes the started app as an argument, so the live test below runs it too.

## A payment without an amount

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` checks the outcome the handler returned.
`assert_not_called` checks that no receipt was published.

## A delay, without waiting for it

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

The test starts on a paused clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it forward.
A minute of `HOLD` costs nothing.
Kinesis cannot hold a record back, so `retry_after` publishes a copy of the record to its stream when the delay ends.
The copy carries the retry count and the record's partition key in its headers, and `published` shows it on `card-payments` next to the test's own record.
The third delivery is the last one the mount site allows, so the record goes to `card-payments.review` as it arrived.
Each held delivery is recorded as `Outcome::Nack`.

## What the in-process Kinesis models

The in-process Kinesis writes and reads records the way the service does for the broker.
The same partition-key rules apply, user headers ride the same envelope, and a delivery carries the same headers and context as one from a shard:

- Every subscription on a stream reads every record of it, and opens at the tip.
- [`start_at`](api:ruststream::runtime::SubscriberSettings::start_at) and the handler's seek handle read the stream's log again from the position they name ([Replay and seeking](replay.md)).
- `retry` with no cap is answered as Kinesis answers it, unsupported, and the record does not come back.
- A deferred retry comes back as a counted copy, and a spent record goes to the dead-letter stream.
- A record the service refuses is refused: a stream name over 128 characters or outside `[a-zA-Z0-9_.-]`, a partition key that is empty or over 256 characters, a record over 10 MiB, the most a stream can be configured to take. A test that publishes one fails with the service's error.
- A publisher or a seek handle used after `shutdown` reports [`KinesisError::NotConnected`](api:ruststream_kinesis::KinesisError::NotConnected).

Every stream has one shard there, so every record is ordered against every other, and every stream a service names exists.
Several shards, leases and checkpoints, retention and resharding need a stack.

## The same test on a stack

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) connects the same app to a running Kinesis, here LocalStack.
A publish waits up to ten seconds for the reaction to settle; [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets a longer deadline.
The test reads the endpoint from `KINESIS_TEST_ENDPOINT` with `support::endpoint`, a helper of the examples, and skips when it is unset.
The service creates `card-payments` when its subscription opens; the test creates `card-receipts`, which only receives.
The subscription opens at the tip, so records an earlier run left in the stream are not read.
The assertions are the in-process ones, unchanged.

```bash
cargo test
KINESIS_TEST_ENDPOINT=http://localhost:4566 cargo test -- --test-threads=1
```
