---
slug: /
sidebar_position: 0
description: "Guides to building Amazon Kinesis Data Streams services with ruststream-kinesis, the Kinesis broker for RustStream."
---

# Guides

`ruststream-kinesis` is the Amazon Kinesis Data Streams broker for
[RustStream](https://powersemmi.github.io/ruststream/latest/), built on the official
[`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis) client. A service reads and writes Kinesis
streams: the crate discovers a stream's shards, leases each shard to one service instance and
checkpoints its progress. The guides take a Kinesis service from installation to production.
Start with [Getting started](getting-started.md).
