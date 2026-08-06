# Kinesis

`ruststream-kinesis` is the Amazon Kinesis Data Streams broker. Kinesis is a sharded, retained log,
so the crate owns what a log consumer needs and the SDK does not provide: shard discovery across
splits and merges, shard leasing with fencing, and per-shard checkpointing. For framework concepts
(writing subscribers, routing, codecs, middleware), see the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.6", features = ["macros", "json"] }
ruststream-kinesis = "0.6"
serde = { version = "1", features = ["derive"] }
```

## Capabilities

Which of the framework's optional capability traits this crate implements natively:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | Yes | `ConnectedKinesisBroker` resolves a string-literal stream name, so `#[subscriber("orders")]` works without a descriptor. See [Subscriptions](#subscriptions). |
| `Seekable` + `Positioned` | Yes | `KinesisSubscriber` mints a `KinesisSeeker`, and `KinesisMessage` reports a `KinesisPosition`. Shard iterators are the service's own repositioning primitive. See [Positions](#positions). |
| `Partitioned` | Yes | `KinesisMessage` exposes the record's partition key, which is the service's unit of shard routing and per-key ordering. See [Publishing](#publishing). |
| `BatchSubscriber` | No | `GetRecords` returns batches, but the reader flattens them into one delivery stream so each record settles against the shard watermark on its own; the framework's batching layer applies unchanged. |
| `RequestReply` | No | Kinesis has no reply address and no correlation primitive; a reply would be a second stream the crate would have to invent. |
| `TransactionalPublisher` | No | The service has no transaction. `PutRecords` is a batch whose entries fail individually, so it cannot provide atomic all-or-nothing publishing. |
| `OwnedTransactions` | No | Same reason: there is no transaction to own. |
| `DescribeServer` | Yes | `KinesisBroker` reports the endpoint (or `kinesis.amazonaws.com`) with the `kinesis` protocol, which is what AsyncAPI generation reads. |

Acknowledgement is not a capability trait, and on this broker it is a per-shard checkpoint rather
than a per-message settlement. See [Leases and checkpoints](#leases-and-checkpoints).

## The lifecycle

The broker is a ladder of consuming transitions, so each state is a distinct type:

```text
KinesisBroker::new()      configuration only, synchronous, no I/O
  .connect()   ->  ConnectedKinesisBroker    the live SDK client; subscriptions and publishers
  .shutdown()  ->  ()                        readers stop and leases lapse
```

`new` performs no I/O - region and credentials resolve on connect - so a Kinesis service is
assembled with the same `#[ruststream::app]` macro as any other broker. `from_config(config)` uses
an AWS config built elsewhere; `endpoint`, `region`, and `test_credentials` cover a local stack.
Because `shutdown` consumes the connected broker, subscribing or publishing after it does not
compile. The SDK client itself has no close, so a publisher handed out earlier reports
`KinesisError::NotConnected` after shutdown instead of quietly writing to a stream the service no
longer consumes.

## Subscriptions

`KinesisStream::new(name)` is the subscription descriptor. It takes a stream name or ARN and sits
inline in the `#[subscriber(..)]` decorator:

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_service.rs:handler"
```

Mount it on the broker; the `with_broker` / `include` part is identical to the in-memory broker.

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_service.rs:app"
```

The descriptor carries the consumer economics, because on Kinesis they are a cost decision rather
than a detail:

| Setting | Default | Meaning |
| --- | --- | --- |
| `batch(n)` | 1000 | Records per `GetRecords` call, capped at 10000. |
| `poll_interval(d)` | 1 second | The pause between reads on an idle shard. The service allows five reads per second per shard; lower values spend that budget faster. |
| `create_if_missing(shards)` | off | Creates the stream with that many shards when it does not exist. Meant for local development and tests; production streams are managed as infrastructure. |

Invalid descriptors are rejected before any I/O.

Behind the descriptor a coordinator lists the stream's shards, re-lists them periodically so splits
and merges are picked up, takes a lease per shard, and runs one reader per owned shard. Children of
a split or merge start only after their parents are fully consumed, which is what keeps per-key
ordering across resharding.

This release polls the shared throughput. Enhanced fan-out is a different resume machine on an
HTTP/2 push stream with no local emulator support, and is not implemented. KPL-aggregated records
are refused with an error rather than delivered to a handler as opaque protobuf.

## Leases and checkpoints

Acknowledgement is a checkpoint, not a per-message settlement. `ack` marks a record handled; the
shard's watermark advances - and is persisted to the lease store - once every earlier record on
that shard is handled too, because a checkpoint implies everything before it.

- `HandlerResult::Ack` marks the record handled.
- `nack(requeue = true)` leaves it unhandled. The watermark stops there, so the shard replays from
  that record when its lease is next taken. A sharded log repositions; it cannot requeue one
  message.
- `nack(requeue = false)` skips the record by checkpointing past it, which is how a poison record
  is retired.

The guarantee is at-least-once: an unacknowledged record wedges the watermark, and everything from
it onward is delivered again after a restart or a lease handover.

Where those checkpoints live is a `LeaseStore`. The trait mirrors the vendor's consumer library:
`acquire` takes a shard for an owner and steals expired leases, `renew` heartbeats it (a failed
renewal means the owner has been fenced and its reader stops immediately), `checkpoint` records
progress conditionally on still holding the lease, `read` reads the persisted state, and `release`
hands the shard back without waiting for expiry. A shard that has been fully consumed is
checkpointed as `SHARD_END`, which is the signal its children may start.

The default store is `MemoryLeaseStore`: in-process, correct for a single service instance, and
nothing survives a restart.

### Sharing shards between instances

The `dynamodb-lease` feature adds `DynamoLeaseStore`, so several instances of a service can share
the shards. The table needs a string partition key named `lease_key` and nothing else; on-demand
billing is enough. Every mutation is a conditional write with a fencing counter, so two instances
cannot both believe they own a shard.

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_leases.rs:leases"
```

`owner_id` names this instance in the table; without it a process-unique value is used.

## Positions

Where a subscription reads from is `KinesisPosition` and nothing else - the descriptor carries no
parallel start enum. By default each shard resumes from its stored checkpoint, and a shard without
one opens at the tip.

| Position | Scope | Meaning |
| --- | --- | --- |
| `KinesisPosition::horizon()` | Stream-wide | The trim horizon: everything the stream still retains. |
| `KinesisPosition::latest()` | Stream-wide | The tip: only records published after the reposition. |
| `KinesisPosition::timestamp(millis)` | Stream-wide | Each shard opens at its first record from that instant, in milliseconds since the Unix epoch. |
| `KinesisPosition::sequence(shard, seq)` | One shard | Exactly one record. |

The stream-wide forms apply to shards discovered later too, including the children of a split, so a
seek does not lose its meaning the moment the stream reshards. The shard-scoped form is the pinned
one the framework defines for captured positions (`Positioned::position` on a delivered record):
seeking to it redelivers that very record, on that shard only, the way a partitioned log seeks per
partition.

`start_at(..)` on the decorator opens the subscription somewhere explicit and beats a stored
checkpoint. A running subscription repositions through the injected `Seek` parameter:

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_seek.rs:seek"
```

Repositioning drops the watermark bookkeeping of every shard it moves, so an acknowledgement of a
record delivered before the seek cannot drag the cursor back over the position just taken. Records
from the new position onward are delivered again, which at-least-once permits. See
[Seeking](https://powersemmi.github.io/ruststream/latest/guides/subscribers/#seeking) in the
framework docs for the capability itself.

## Publishing

`KinesisPublish` is the broker's publish policy and its default one, so a
`#[subscriber(.., publish("dest"))]` handler mounted without an explicit publisher replies through
it. It pairs into `KinesisPublisher`, whose destination is the stream name or ARN.

The `partition-key` header becomes the record's own partition key - the unit of shard routing and
therefore of per-key ordering. Without one, a process-unique key spreads records across shards. The
same header is set on every delivered record, and it feeds the framework's `Partitioned`
capability, so the convention matches the in-memory broker and a service can switch brokers without
changing its headers.

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_seek.rs:publish"
```

Deliveries additionally expose `kinesis-sequence-number` and `kinesis-shard-id`
(`SEQUENCE_HEADER` and `SHARD_HEADER`).

## The header envelope

A Kinesis record carries only a data blob and a partition key, so user headers beyond the partition
key ride a small envelope, applied only when such headers exist:

- A record published with no user headers is written as the plain payload, readable by any Kinesis
  consumer, and records written by other producers are read back as headerless.
- Otherwise the blob is the four-byte magic `RSK1`, a big-endian `u32` header-block length, the
  header block, and the payload.

The partition key never enters the envelope, since it travels natively on the record.

## Running against LocalStack

The crate is developed against [LocalStack](https://localstack.cloud/), which emulates Kinesis
locally. `just brokers-up` starts the container defined in `docker-compose.test.yml` and
`just brokers-down` stops it. Point the broker at it with the local-stack triple:

```text
KinesisBroker::new()
    .endpoint("http://localhost:4566")
    .test_credentials()
    .region("us-east-1")
```

The image is pinned to the last token-free tag; newer LocalStack images require an auth token,
which fork pull requests cannot read from secrets. The emulator injects 500ms of artificial latency
per call by default, so the compose file sets `KINESIS_LATENCY=0`.

## Testing

The `testing` feature ships `KinesisTestBroker`: an in-process transport that reproduces the
crate's core routing with no server and no network. It follows the same ladder as the real broker,
and its connected form implements `ruststream::testing::TestableBroker`, so it drives the `TestApp`
harness: inject traffic with `broker.inject(OutgoingMessage::new(..))` and assert on published
output with the free `ruststream::testing::expect_published`. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

It routes by exact address match and simulates none of the product behaviour. Shard leases,
checkpoint resume, replay of unacknowledged records, and resharding are covered by the live suite
instead, gated behind `KINESIS_TEST_ENDPOINT`:

```text
just test-brokers
```

That starts LocalStack and runs the integration tests plus the framework's conformance lifecycle
against it, single-threaded so the runs do not observe each other's streams.
