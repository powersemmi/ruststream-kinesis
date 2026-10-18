---
sidebar_position: 9
description: "Run a Kinesis service on AWS: credentials and region from the environment or a config of your own, streams owned by infrastructure, and its IAM permissions."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Running in production

This guide runs a Kinesis service on AWS: where its credentials and region come from, which streams it expects to find, and the IAM permissions it needs.

## Credentials and region from the environment

`KinesisBroker::new()` takes nothing from the code.
The AWS SDK's [default chain](../components/broker/credentials.md) finds the region and the credentials when the service starts: environment variables, the shared `~/.aws` files, SSO, and the role of the container or the instance.
A service that runs under an IAM role on ECS, EKS or EC2 needs no configuration at all.
The broker builds its configuration with the SDK's newest behaviour version.

The SDK reads credentials when it signs its first request.
That request is the check of the first subscription's stream, at startup, so a service with wrong credentials stops before it handles a record.

## A configuration of your own

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#config"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#config"
```

</TabItem>
</Tabs>

[`KinesisBroker::from_config`](api:ruststream_kinesis::KinesisBroker::from_config) takes an `SdkConfig` the service builds.
`aws_config` builds one from the environment with an `async` loader, while the app function is synchronous.
So the example builds it with `SdkConfig::builder()`, which does no I/O.
The profile provider reads the `orders-service` profile on the first request; a profile with a `role_arn` assumes that role.
The pinned behaviour version keeps the SDK's defaults from changing under the service when the SDK is updated.
The broker uses the configuration as it is: [`endpoint`, `region` and `test_credentials`](../components/broker/endpoint.md) apply to `KinesisBroker::new()` only, and a configuration of your own names them with `endpoint_url`, `region` and `credentials_provider` on the builder.
A [`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md) takes the same configuration ([Several instances](leases.md)).
The example needs two more dependencies:

```toml title="Cargo.toml"
[dependencies]
aws-config = "1"
aws-sdk-kinesis = "1"
```

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

The `orders` stream belongs to the infrastructure: Terraform, CloudFormation or the console create it, with its capacity mode, retention and encryption.
A stream is named by its name; the account and the region come from the configuration.
The descriptor leaves out [`create_if_missing`](../components/subscriptions/create-if-missing.md), so a missing stream stops the service at startup with an error that names it, instead of creating an empty one.
`create_if_missing(n)` creates a provisioned stream with `n` shards, which suits a local stack; whether a production stream is provisioned or on demand is the operator's choice.

Requests go over the SDK's default HTTPS client, and the SDK retries a failed request with its own retry policy.
A handler on a dedicated thread (`threads(n)`) sends through an SDK client of its own, built from the same configuration and sharing its credentials.

## IAM permissions

The service above needs these permissions on its stream:

```json title="policy.json"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStreamSummary",
        "kinesis:ListShards",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords"
      ],
      "Resource": "arn:aws:kinesis:eu-west-1:123456789012:stream/orders"
    }
  ]
}
```

Each feature adds its own:

| Feature | Permissions |
|---|---|
| a publish, a reply or a retry copy | `kinesis:PutRecord` on the stream it goes to |
| `create_if_missing` | `kinesis:CreateStream` and `kinesis:DescribeStream` |
| `DynamoLeaseStore` | `dynamodb:GetItem` and `dynamodb:UpdateItem` on the lease table |

## What the AsyncAPI document shows

With the `asyncapi` feature, the service's AsyncAPI document names one [server](../components/asyncapi/server.md): the host of the broker's `endpoint`, `kinesis.<region>.amazonaws.com` when `region` names one, or `kinesis.amazonaws.com`.
The document never holds a credential, so it can be published.

The tests in the example file start the same app in process; the in-process Kinesis signs nothing and reads no profile.
[Testing a Kinesis service](testing.md) covers the harness.
