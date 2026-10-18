---
sidebar_position: 2
title: Endpoint
description: "endpoint, region and test_credentials point the broker at a local stack such as LocalStack; each overrides one setting of the default chain."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Endpoint

[`endpoint`](api:ruststream_kinesis::KinesisBroker::endpoint), [`region`](api:ruststream_kinesis::KinesisBroker::region) and [`test_credentials`](api:ruststream_kinesis::KinesisBroker::test_credentials) point the broker at a local stack such as LocalStack; each overrides one setting of the default chain.

`endpoint` replaces the AWS endpoint with an address such as `http://localhost:4566`, where LocalStack listens.
`region` sets the region every request is signed for.
`test_credentials` signs requests with a fixed dummy key: a local stack checks that a request is signed and ignores the key.
Each step overrides only its own setting, and the SDK's [default chain](./credentials.md) resolves the rest.
The three steps apply to a broker built with `new`; a broker built with `from_config` takes all three from its config.

The generated [AsyncAPI document](../asyncapi/server.md) reports the endpoint's host and port as the server.
A broker with no endpoint and a named region is reported as that region's Kinesis host, `kinesis.<region>.amazonaws.com`.

## Example

The service runs against a local stack on port 4566.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_broker_endpoint.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_broker_endpoint.rs#broker"
```

</TabItem>
</Tabs>
