---
sidebar_position: 1
title: Server
description: "The server reports the endpoint's host and port with the protocol kinesis, the regional host when a region is named, and never a credential."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server reports the endpoint's host and port with the protocol `kinesis`, the regional host when a region is named, and never a credential.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
Its `protocol` is `kinesis`, and its `host` comes from the first of these that applies:

1. a broker with an [`endpoint`](../broker/endpoint.md) reports that URL's host and port; the scheme and any user name and password stay out;
2. a broker with a [`region`](../broker/endpoint.md) reports that region's host, `kinesis.<region>.amazonaws.com`;
3. any other broker reports `kinesis.amazonaws.com`, because its region resolves from the environment only when it connects.

The host comes from the broker's own steps.
A broker built with [`from_config`](../broker/credentials.md) connects where its config points and reports a host from its steps all the same.

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
