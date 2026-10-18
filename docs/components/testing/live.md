---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test body against LocalStack or AWS, for shards, leases, checkpoints and resharding."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test body against LocalStack or AWS, for shards, leases, checkpoints and resharding.

It connects the broker through its ordinary connect, so every subscription describes its stream, lists its shards and takes its leases for real.
The body is the one an in-process test runs; only the start call differs.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what was published to it.
The wait lasts ten seconds at most, and [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound.

The live mode is for what the in-process transport leaves out: a stream with several [shards](../subscriptions/shards.md), [leases](../leases/index.md) shared by two instances, checkpoints that outlive a restart, resharding and retention.
A stand keeps what earlier runs left in it.
The streams a service reads must exist before the app starts, unless its descriptors [create them](../subscriptions/create-if-missing.md).
A subscription with no checkpoint opens at the tip, so the records earlier runs left behind are not read.
A test that reads the retained log, such as one on [`start_at`](../positions/start-at.md), starts from a stream it recreated or from a timestamp taken when the test began.

In the example, `stand::endpoint` returns the stand's address from `KINESIS_TEST_ENDPOINT`, and the test returns early without one, so `cargo test` passes on a machine with no stand.
A job that stood a stand up sets `RUSTSTREAM_REQUIRE_LIVE=1`, and a missing address then fails the test instead of skipping it.
The repository's stand is `docker-compose.test.yml`, LocalStack with Kinesis and DynamoDB, which `just brokers-up` starts.

## Example

The two tests share one body: one in process, one against the stand at `KINESIS_TEST_ENDPOINT`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_testing_live.rs#start-live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_testing_live.rs#start-live"
```

</TabItem>
</Tabs>
