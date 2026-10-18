---
sidebar_position: 1
title: Credentials
description: "Credentials and region resolve through the AWS default chain at connect; from_config takes a prebuilt SdkConfig, which a DynamoDB lease store shares."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Credentials

Credentials and the region resolve through the AWS SDK's default chain at connect; [`from_config`](api:ruststream_kinesis::KinesisBroker::from_config) takes a prebuilt `SdkConfig`, which a [DynamoDB lease store](../leases/dynamo-lease-store.md) shares.

A broker built with [`new`](api:ruststream_kinesis::KinesisBroker::new) loads its configuration with `aws_config` when the runtime connects it.
The region comes from `AWS_REGION` or the shared profile.
The credentials come from the environment, the shared profile files, SSO, or the instance or container role.
The SDK fetches them on the first request, a subscription's `DescribeStreamSummary` at [startup](./startup.md), so a service with a subscription meets a missing credential there.
The configuration follows the SDK's latest behaviour version.

`from_config` takes an `aws_config::SdkConfig` the service builds itself: a named profile, a pinned behaviour version, its own HTTP client or credential provider.
The broker uses that config as it is.
The region, the credentials and the endpoint all come from it, and the broker's own [`endpoint`, `region` and `test_credentials`](./endpoint.md) steps do not apply.
A [`DynamoLeaseStore`](../leases/dynamo-lease-store.md) is built from an `SdkConfig` too, so a service that uses one hands the same config to both.
Transport security is the SDK's HTTPS client in both cases.

The service's role needs these permissions:

| Action | When |
|---|---|
| `kinesis:DescribeStreamSummary`, `kinesis:ListShards`, `kinesis:GetShardIterator`, `kinesis:GetRecords` | every subscription |
| `kinesis:CreateStream`, `kinesis:DescribeStream` | a subscription with [`create_if_missing`](../subscriptions/create-if-missing.md) |
| `kinesis:PutRecord` | every publish |
| `dynamodb:GetItem`, `dynamodb:UpdateItem` | a [`DynamoLeaseStore`](../leases/dynamo-lease-store.md), on its table |

## Example

The service signs its requests with the `orders` profile of the shared AWS files, in `eu-west-1`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_broker_credentials.rs#config"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_broker_credentials.rs#config"
```

</TabItem>
</Tabs>
