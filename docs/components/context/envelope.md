---
sidebar_position: 3
title: Envelope
description: "Headers ride the record's data in the layout of an envelope: the default RSK2 block, a codec document, no envelope at all, or a layout of the service's own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Envelope

Headers ride the record's data in the layout of an envelope: the default `RSK2` block, a codec document, no envelope at all, or a layout of the service's own.

A Kinesis record carries a data blob and a partition key and nothing else.
The partition key travels in the record's own [partition-key field](../publishing/keys.md) whatever the envelope.
The other headers travel in the data, in the layout of an [`Envelope`](api:ruststream_kinesis::Envelope).
The crate ships three:

| Envelope | The record's data | Read by |
|---|---|---|
| [`Rsk`](api:ruststream_kinesis::Rsk), the default | the payload, with a binary header block in front of it when there are headers | every release of this crate |
| [`Framed`](api:ruststream_kinesis::Framed) | one document of a RustStream codec: `headers` and `payload` | any consumer with a JSON, CBOR or msgpack library |
| [`NoOp`](api:ruststream_kinesis::NoOp) | the payload alone | any consumer |

The envelope is chosen the way a codec is, and the most specific choice wins.
[`KinesisBroker::envelope`](api:ruststream_kinesis::KinesisBroker::envelope) sets the broker's.
A subscription overrides it with `.envelope(..)` after the handler at the mount site, or on its [`KinesisStream`](api:ruststream_kinesis::KinesisStream) on the manual path.
A publish position overrides it on its policy: `Publish::default().envelope(..)`.
The choice is a type, so it costs nothing per record.

A delivery finds its payload when it is made and reads the headers only when something asks for them.
A handler that reads no header never reads the header block.

A record the envelope does not recognise is somebody else's payload: it reaches the handler whole, with no headers.

A record aggregated by the Kinesis Producer Library packs many user records into one.
The subscription reports it as [`KinesisError::AggregatedRecord`](api:ruststream_kinesis::KinesisError::AggregatedRecord), which the runtime logs, and moves on: the record is never handed to a handler, and the shard's checkpoint moves past it.

## The default envelope

A record with no headers is the plain payload.
A payload that itself begins with `RSK2` or `RSK1` gets an envelope with an empty header block, so it reads back whole instead of being taken for one.
The block is the four bytes `RSK2`, a big-endian `u32` length of the block, then each header as a big-endian `u32` name length, the name, a big-endian `u32` value length and the value.
So a value comes back byte for byte.
Records written by earlier releases in the `RSK1` text envelope still read back with their headers.
A record without the magic, or whose header block does not parse, reads as a payload with no headers.

## A codec document

Every record is one document, with or without headers: `{"headers": {"x-tenant": "acme"}, "payload": [...]}` in JSON.
Header values are text, and a publish whose header value is not UTF-8 fails.
The payload is a byte string in CBOR and msgpack and an array of numbers in JSON.

The payload is not a slice of the record, so a delivery decodes it into a buffer of its own: one allocation per record that the binary envelope does not make.
The generated AsyncAPI document reports the envelope and the media type of its document beside the broker, the subscription or the policy that names it.

## No envelope

The record's data is the payload, byte for byte: what a Firehose delivery stream, a Lambda or a service in another language reads.
A delivery carries the partition key, the [sequence number and the shard](headers.md) and no other header.
A publish that carries another header fails with [`KinesisError::Envelope`](api:ruststream_kinesis::KinesisError::Envelope), which names the header and the stream.
That includes the attempt counter of `max_attempts(..)`, so a registration that counts its attempts does not publish its copies through this envelope.

## An envelope of your own

A layout another system already writes is an [`Envelope`](api:ruststream_kinesis::Envelope) of the service.
`seal` writes a record, `body` finds the payload in one, and `read_headers` reads its headers on the first request.

## Testing

The harness's [`TestApp`](../testing/index.md) writes a record the way the broker's envelope writes it.
It knows `KinesisBroker` in the default envelope and in `NoOp`, and in `Framed` over each codec whose feature the crate has on (`json`, `cbor`, `msgpack`).
A test of a broker in an envelope of the service's own registers that broker once: `ruststream::register_testable_broker!(KinesisBroker<TenantLine>);`.

## Example

### The default envelope

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

### A codec document

The order and its receipt both travel as JSON documents, the tenant header inside them.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_context_envelope_framed.rs#framed"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_context_envelope_framed.rs#framed"
```

</TabItem>
</Tabs>

### No envelope

A browser SDK writes each click as a plain JSON record.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_context_envelope_noop.rs#noop"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_context_envelope_noop.rs#noop"
```

</TabItem>
</Tabs>

### An envelope of your own

Here the layout is one `tenant=<name>` line in front of the payload.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/kinesis_context_envelope_custom.rs#custom"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/kinesis_context_envelope_custom.rs#custom"
```

</TabItem>
</Tabs>
