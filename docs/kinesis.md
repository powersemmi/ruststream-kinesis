# Kinesis

`ruststream-kinesis` runs a RustStream service on Amazon Kinesis Data Streams. Kinesis is a
sharded, retained log. A subscription reads from any position the stream still retains, and it
repositions while it runs. Acknowledgement is a per-shard checkpoint, and one instance reads a
shard at a time.

Over the SDK the crate supplies what a log consumer needs: shard discovery across splits and
merges, shard leasing with fencing, and per-shard checkpointing. For framework concepts (writing
subscribers, routing, codecs, middleware), see the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
```

## Capabilities

The framework's optional capability traits, and what this crate does with each:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | Yes | A string literal names the stream, so `#[subscriber("orders")]` mounts without a descriptor. See [Subscriptions](#subscriptions). |
| `Seekable` + `Positioned` | Yes | A handler reads where its record sits and repositions the subscription, both through the delivery context. See [Positions](#positions). |
| `Partitioned` | Yes | A delivered record reports its partition key: the key that picked its shard, and the one it stays ordered under. See [Publishing](#publishing). |
| `BatchSubscriber` | Yes | The batch size a mount site names becomes the `GetRecords` limit, so one read fetches at most one batch's worth. See [Batches](#batches). |
| `RequestReply` | No | Kinesis has no reply address and no correlation primitive; a reply would be a second stream the crate would have to invent. |
| `TransactionalPublisher` | No | The service has no transaction: the entries of a `PutRecords` call succeed and fail one by one. |
| `OwnedTransactions` | No | Same reason: there is no transaction to own. |
| `DescribeServer` | Yes | The generated AsyncAPI document names the configured endpoint, or `kinesis.amazonaws.com`, under the `kinesis` protocol. |

Acknowledgement is not a capability trait, so the table leaves it out; see
[Leases and checkpoints](#leases-and-checkpoints) for what `ack` does here.

`ruststream_kinesis::prelude` carries the framework's own prelude and this crate's surface in one
glob: `Seeker` to reposition a subscription, `Positioned` to read a delivered record's position,
and the delivery contexts `KinesisContext` and `KinesisBatchContext` with the keys `Position` and
`SeekHandle` that read them. A record's partition key is read through
`IncomingMessage::partition_key`.

## The lifecycle

Each transition consumes the state before it, so every state is a distinct type:

```text
KinesisBroker::new()      configuration only, synchronous, no I/O
  .connect()   ->  ConnectedKinesisBroker    the live SDK client; subscriptions and publishers
  .shutdown()  ->  ()                        readers stop and leases lapse
```

`new` does no I/O: region and credentials resolve when the runtime connects the broker.
`from_config(config)` takes an AWS config built elsewhere, and `endpoint`, `region` and
`test_credentials` point the broker at a local stack.

`shutdown` consumes the connected broker, so subscribing or publishing after it does not compile.
A publisher handed out earlier outlives the connection, and returns `KinesisError::NotConnected`
rather than writing to a stream the service no longer consumes.

## Subscriptions

`KinesisStream::new(name)` is the subscription descriptor: one stream, named by name or by ARN. It
sits inline in the `#[subscriber(..)]` decorator:

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_service.rs:handler"
```

Mount it on the broker:

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_service.rs:app"
```

A service built without the `macros` feature hands the same descriptor to the `subscriber(source,
handler)` constructor.

The descriptor also carries what the consumer costs, which on Kinesis is a decision rather than a
detail:

| Setting | Default | Meaning |
| --- | --- | --- |
| `poll_interval(d)` | 1 second | The pause between reads on an idle shard. The service allows five reads a second per shard, and a shorter pause spends that budget faster. |
| `create_if_missing(shards)` | off | Creates the stream with that many shards when it is missing. Meant for local development and tests; a production stream is managed as infrastructure. |

Both are available at the mount site too, through the `KinesisSubscriberExt` trait the prelude
carries: `b.include(digest.batch(nonzero!(500)).poll_interval(..))`. They transform the descriptor,
so they need one to transform: `start_at(..)` replaces it with the framework's position wrapper,
and they chain before it. A subscriber whose attribute already names a start position sets them on
the descriptor instead, which is the same two methods on the same type.

An invalid descriptor is rejected before any I/O.

A subscription lists the stream's shards, re-lists them as splits and merges change the set, takes
a lease per shard, and runs one reader per shard it owns. A child of a split or a merge starts only
after its parents are fully consumed, which is what keeps per-key ordering across resharding.

Reads go through the stream's shared throughput; enhanced fan-out is not implemented. A
KPL-aggregated record is returned as an error instead of reaching a handler as opaque protobuf.

## Batches

A handler that takes a slice consumes a batch, and its mount site names the batch size - the one
subscription parameter the framework carries down to a broker. Here it becomes the `GetRecords`
limit every shard reader asks with, so one read fetches at most one batch's worth and no batch
carries more records than it named:

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_batches.rs:batches"
```

A batch carries fewer records when that is all the shards had, and a batch still filling is
delivered 50 ms after its first record.

One batch mixes records from every shard this instance owns. Each record settles on its own against
its shard's watermark, so a batch body that returns one outcome per element checkpoints per record.

The size is a cost decision as well: a small batch means small reads, against the same five reads a
second per shard. `poll_interval(..)` is where that budget is spent. A read the service throttles
is not a delivery failure: the reader waits one interval and reads again from where it stopped, so
a handler sees a pause rather than an error.

A batch body reads `KinesisBatchContext` rather than `KinesisContext` - see
[Positions](#positions).

## Leases and checkpoints

`ack` marks a record handled. A checkpoint covers every record before it, so the shard's watermark
advances, and is written to the lease store, only when no earlier record on that shard is still
unhandled.

- `HandlerOutcome::ack()` marks the record handled.
- `nack(requeue = true)` leaves it unhandled. The watermark stops there, so the shard replays from
  that record when its lease is next taken. A sharded log repositions; it cannot requeue one
  record.
- `nack(requeue = false)` checkpoints past the record, which is how a poison one is retired.

Delivery is at-least-once: an unacknowledged record holds the watermark where it is, and everything
from it onward is delivered again after a restart or a lease handover.

Checkpoints live in a `LeaseStore`. `acquire` takes a shard for an owner and steals a lease that
has expired, `renew` heartbeats it, `checkpoint` records progress while the owner still holds the
lease, `read` returns the persisted state, and `release` hands the shard back without waiting for
the expiry. A failed renewal means another owner has taken the shard, and that reader stops
immediately. A fully consumed shard is checkpointed as `SHARD_END`, which is the signal its
children may start.

The default store is `MemoryLeaseStore`: in process, correct for a single service instance, and
empty again after a restart.

### Sharing shards between instances

The `dynamodb-lease` feature adds `DynamoLeaseStore`, and several instances of a service then share
the shards. The table needs a string partition key named `lease_key` and nothing else; on-demand
billing is enough. Every write is conditional and bumps a fencing counter, so two instances cannot
both hold one shard.

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_leases.rs:leases"
```

`owner_id` names this instance in the table; left out, it is a process-unique value.

## Positions

`KinesisPosition` is the whole vocabulary for where a subscription reads from. By default each
shard resumes from its stored checkpoint, and a shard without one opens at the tip.

| Position | Scope | Meaning |
| --- | --- | --- |
| `KinesisPosition::horizon()` | Stream-wide | The trim horizon: everything the stream still retains. |
| `KinesisPosition::latest()` | Stream-wide | The tip: only records published after the reposition. |
| `KinesisPosition::timestamp(millis)` | Stream-wide | Each shard opens at its first record from that instant, in milliseconds since the Unix epoch. |
| `KinesisPosition::sequence(shard, seq)` | One shard | Exactly one record. |

A stream-wide position reaches shards discovered later too, the children of a split among them, so
a seek keeps its meaning when the stream reshards. The shard-scoped form is the pinned position the
framework captures from a delivered record (`Positioned::position`): seeking to it redelivers that
record, and moves no other shard.

`start_at(..)` on the decorator opens the subscription at a position you name, ahead of any stored
checkpoint. A running subscription repositions from a handler: this broker fills the delivery
context with the record's position and the subscription's seeker, and `Ctx<SeekHandle>` binds that
seeker as a handler parameter:

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_seek.rs:seek"
```

`Ctx<Position>` binds the record's own pinned position the same way, and a handler that wants both
names `KinesisContext` as its context type and reads the keys with `ctx.context(..)`.

A batch handler gets `KinesisBatchContext`. It carries the same `SeekHandle`, because a seek moves
the whole subscription, and no position, because a batch spans many records; a body that reacts to
a position reads the `kinesis-sequence-number` and `kinesis-shard-id` headers of its elements.

A reposition drops the watermark bookkeeping of every shard it moves, so an acknowledgement of a
record delivered before the seek cannot pull the cursor back over the position just taken. Records
from the new position onward are delivered again, which at-least-once permits. The capability
itself is [Seeking](https://powersemmi.github.io/ruststream/latest/guides/subscribers/#seeking) in
the framework docs.

## Publishing

`KinesisPublish` is the policy that constructs `KinesisPublisher`, and the runtime instantiates one
on the connected broker at startup. It is also this broker's default policy, so a replying handler
mounted without an explicit policy replies through it. `.out(Reply, Publish::default())` at the
mount site names it explicitly, and the same call binds the publisher of an injected slot when the
marker is the slot's instead of `Reply`.

A handler body imports the framework alone (`use ruststream::prelude::*`) and bounds an injected
publisher with a capability trait, so it never learns which broker it runs on. The file that mounts
those handlers imports `use ruststream_kinesis::prelude::*`, where this broker's policy carries the
uniform mount-site name `Publish`; `KinesisPublish` stays at the crate root for a file that mounts
two brokers.

The publish builder is the framework's: `message(..)` with a declared type, then `to(..)` with the
stream name or ARN, `with_headers(..)`, `with_codec(..)` and `publish()`. A payload the service
already holds encoded is declared as a `#[derive(Outgoing, Serialized)]` newtype: no codec runs on
it, and the generated document still names the message. See the
[publishing guide](https://powersemmi.github.io/ruststream/latest/guides/publishing/).

`KinesisPublishExt::with_partition_key` sets the record's partition key. It is called on the
publisher, before `message(..)`, and returns a `PartitionKeyed<KinesisPublisher>` that the builder
continues from unchanged:

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_seek.rs:publish"
```

A reply and an injected slot publish through a publisher the service never holds, since the runtime
constructs it from the policy. Their key is named on that policy, at the mount site, through the
`KinesisPublishSettings` trait the prelude carries:

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_replies.rs:replies"
```

One key means one shard, and with it that shard's throughput. Name a key when the records have to
stay mutually ordered, and leave it off otherwise: with no key anywhere, a process-unique one
spreads the records across the shards.

A key can be named in three places, and they resolve in one order. A record that carries the
`partition-key` header decides its own key, which is what `with_partition_key` sets; otherwise the
key the mount site named on the policy applies; otherwise the record spreads. Setting that header
by hand works as well, and a publish that names other headers keeps the key already decided
alongside them.

The header is how the crate spells the key: the publisher sets the record's own partition key from
it, and a delivered record carries it back in the same header. Deliveries also carry
`kinesis-sequence-number` and `kinesis-shard-id` (`SEQUENCE_HEADER` and `SHARD_HEADER`).

## The header envelope

A Kinesis record carries only a data blob and a partition key, so user headers beyond the partition
key are written into a small envelope around the payload, and only when such headers exist:

- A record published with no user headers is the plain payload, which any Kinesis consumer reads,
  and a record written by another producer is read back as headerless.
- Otherwise the blob is the four-byte magic `RSK1`, a big-endian `u32` header-block length, the
  header block, and the payload.

The partition key stays out of the envelope: the record has a field of its own for it.

## Running against LocalStack

[LocalStack](https://localstack.cloud/) emulates Kinesis on the development machine.
`just brokers-up` starts the container `docker-compose.test.yml` defines, and `just brokers-down`
removes it. Three settings point the broker at it:

```text
KinesisBroker::new()
    .endpoint("http://localhost:4566")
    .test_credentials()
    .region("us-east-1")
```

The compose file pins the last token-free image, because newer LocalStack images require an auth
token that a fork's pull request cannot read from secrets. It also sets `KINESIS_LATENCY=0`; the
emulator otherwise adds 500 ms to every call.

## Testing

The `testing` feature ships `KinesisTestBroker`: an in-process transport with no server and no
network. It has the same states as the real broker and drives the `TestApp` harness. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

The transport keeps a retained log per stream, because that is the property a handler can observe
without a server. A subscription opens at the tip, where a shard without a checkpoint opens, and
`start_at(..)` or a handler's `SeekHandle` really re-reads the log from the position it names. A
delivery carries the full surface: a `KinesisPosition`, the `kinesis-sequence-number` and
`kinesis-shard-id` headers, the partition key, and the `KinesisContext` or `KinesisBatchContext`
the keys read. So a service mounts unchanged:

```rust
--8<-- "crates/ruststream-kinesis/tests/harness_kinesis.rs:seek_handler"
```

```rust
--8<-- "crates/ruststream-kinesis/tests/harness_kinesis.rs:seek_test"
```

The framework's own seeking and batch suites run against the transport in process. Batches are
grouped by the framework's client-side adapter, so a batch mount is the same mount here and against
the service.

What it does not have is what a server owns: it routes one shard (`testing::IN_PROCESS_SHARD`), so
there are no leases, no checkpoint durability, no retention limits, no resharding, and no
redelivery timing. The live suite covers those, gated behind `KINESIS_TEST_ENDPOINT`:

```text
just test-brokers
```

That starts LocalStack and runs the wire and lease checks against it, together with the framework's
lifecycle, `Seekable` and batch suites, single-threaded so the runs do not observe each other's
streams.
