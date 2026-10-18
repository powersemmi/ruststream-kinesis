---
sidebar_position: 3
title: Envelope
description: "User headers ride an RSK2 envelope in front of the payload, only when there are any; a record from another producer reads as headerless."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Envelope

User headers ride an `RSK2` envelope in front of the payload, only when there are any; a record from another producer reads as headerless.

A Kinesis record carries a data blob and a partition key and nothing else.
So the publisher writes the headers a message carries into a small envelope in front of the payload.
The partition key stays out of it: it travels in the record's own [partition-key field](../publishing/keys.md).
A record with no other headers is the plain payload, which any Kinesis consumer reads as it is.

The envelope is the four bytes `RSK2`, a big-endian `u32` length of the header block, the header block, and then the payload.
The block holds each header as a big-endian `u32` name length, the name, a big-endian `u32` value length and the value.
So a value comes back byte for byte, whatever bytes it holds.
Records written by earlier releases in the `RSK1` text envelope still read back with their headers.
A record without the magic, or whose header block does not parse, reads as a payload with no headers, so a record from another producer reaches the handler as that producer wrote it.

A record aggregated by the Kinesis Producer Library packs many user records into one.
The subscription reports it as [`KinesisError::AggregatedRecord`](api:ruststream_kinesis::KinesisError::AggregatedRecord), which the runtime logs, and moves on: the record is never handed to a handler, and the shard's checkpoint moves past it.

## Example

A payment's signature, raw bytes no text header could carry, travels in the envelope and reaches the handler unchanged.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_context_envelope.rs#envelope"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_context_envelope.rs#envelope"
```

</TabItem>
</Tabs>
