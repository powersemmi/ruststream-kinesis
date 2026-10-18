---
sidebar_position: 1
title: Server
description: "The server reports the host clients connect to, the endpoint's or the region's, with the protocol kinesis, and never a credential."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server reports the host clients connect to, the endpoint's or the region's, with the protocol `kinesis`, and never a credential.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
Its `protocol` is `kinesis`, and its `host` comes from the first of these that applies:

1. a broker with an [`endpoint`](../broker/endpoint.md) reports that URL's host and port; the scheme and any user name and password stay out;
2. a broker with a [`region`](../broker/endpoint.md) reports that region's host, such as `kinesis.eu-west-1.amazonaws.com`, or `kinesis.cn-north-1.amazonaws.com.cn` in the China partition;
3. any other broker reports `kinesis.amazonaws.com`.

The endpoint and the region are the ones named on the broker, or else the ones of the config given to [`from_config`](../broker/credentials.md).
A region the SDK resolves from the environment is known only once the service connects, after the document is built, so such a broker reports `kinesis.amazonaws.com`.

## Example

The broker runs in `eu-west-1`, and the document reports `kinesis.eu-west-1.amazonaws.com`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_asyncapi.rs#server"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_asyncapi.rs#server"
```

</TabItem>
</Tabs>
