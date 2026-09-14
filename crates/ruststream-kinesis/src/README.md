Amazon Kinesis Data Streams for `RustStream`: a sharded, retained log behind the framework's
subscribers and publishers.

A Kinesis stream is a log cut into shards, and a record stays in it until the retention window
passes. This crate runs the transport over the official
[`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis) client and adds what the vendor's consumer
library gives other platforms and the Rust SDK does not: shard discovery across splits and merges,
shard leasing with fencing, and per-shard checkpointing. Acknowledgement is a checkpoint, one
instance reads a shard at a time, and delivery is at-least-once.

Handlers, routers, codecs, middleware and the application object are the framework's and are
documented with it: <https://docs.rs/ruststream>. Installation, the tutorial and the list of
brokers are on the site: <https://powersemmi.github.io/ruststream/>.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
```

# The first service

A handler is an `async fn` over a decoded record. [`KinesisStream`] names the stream it reads,
[`KinesisBroker`] carries the connection settings, and the attribute writes the `main`:

```
# mod demo {
use ruststream_kinesis::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(KinesisStream::new("orders"))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(handle);
    })
}
# }
# fn main() {}
```

`cargo run -- run` starts it. [`KinesisBroker::new`] is synchronous and records configuration
only: the region and the credentials resolve from the environment when the runtime connects the
broker, which is what lets the service compose with the synchronous `#[ruststream::app]` builder.
[`from_config`](KinesisBroker::from_config) takes an AWS config built elsewhere, and
[`endpoint`](KinesisBroker::endpoint), [`region`](KinesisBroker::region) and
[`test_credentials`](KinesisBroker::test_credentials) point the broker at a local stack.

Each lifecycle transition consumes the state before it, so every state is a distinct type:
`KinesisBroker::new()` is configuration, `connect` yields [`ConnectedKinesisBroker`], and
`shutdown` consumes that. Subscribing or publishing after `shutdown` does not compile. A publisher
handed out earlier outlives the connection and reports [`KinesisError::NotConnected`] rather than
writing into a stream the service no longer consumes.

# Subscribing

## The subscription source

[`KinesisStream::new(name)`](KinesisStream::new) is the descriptor: one stream, named by name or
by ARN, carrying every setting that subscription form has. It sits inline in the attribute, and a
service built without the `macros` feature hands the same value to the `subscriber(source,
handler)` constructor.

| Setting | Default | Meaning |
| --- | --- | --- |
| [`poll_interval(d)`](KinesisStream::poll_interval) | 1 second | The pause between reads on an idle shard. The service allows five reads a second per shard, and a shorter pause spends that budget faster. |
| [`create_if_missing(shards)`](KinesisStream::create_if_missing) | off | Creates the stream with that many shards when it is missing, then waits up to a minute for it to become usable. Meant for local stacks and tests; a production stream is managed as infrastructure. |

Both settings are also mount-site steps, through [`KinesisSubscriberExt`]; the
[batch example](#batches) below mounts one that way. They transform the descriptor, so they chain
before `start_at(..)`, which replaces it with the framework's position wrapper.

A string literal names a stream too: `#[subscriber("orders")]` builds the same descriptor with its
defaults. Reach for the descriptor as soon as a read setting matters.

An invalid descriptor - an empty stream name, a zero shard count - fails the mount before any I/O.

A subscription lists the stream's shards, re-lists them as splits and merges change the set, takes
a lease per shard, and runs one reader per shard it owns. A child of a split or a merge starts only
after its parents are fully consumed, which is what keeps per-key ordering across resharding.

Reads go through the stream's shared throughput; enhanced fan-out is not implemented. A
KPL-aggregated record is reported as [`KinesisError::AggregatedRecord`] rather than handed to a
handler as opaque protobuf.

## Settling a record

`ack` marks a record handled. A checkpoint covers every record before it, so the shard's watermark
advances - and is written to the lease store - only when no earlier record on that shard is still
unhandled.

- `HandlerOutcome::ack()` marks the record handled.
- `HandlerOutcome::retry()` leaves it unhandled. The watermark stops there, so the shard replays
  from that record when its lease is next taken. A sharded log repositions; it cannot requeue one
  record.
- `HandlerOutcome::drop()` checkpoints past the record, which is how a poison one is retired.

Delivery is at-least-once: an unacknowledged record holds the watermark where it is, and everything
from it onward is delivered again after a restart or a lease handover.

## Leases and checkpoints

Checkpoints live in a [`LeaseStore`]. [`acquire`](LeaseStore::acquire) takes a shard for an owner
and steals a lease that has expired, [`renew`](LeaseStore::renew) heartbeats it,
[`checkpoint`](LeaseStore::checkpoint) records progress while the owner still holds the lease,
[`read`](LeaseStore::read) returns the persisted [`LeaseState`], and
[`release`](LeaseStore::release) hands the shard back without waiting for the expiry. A failed
renewal means another owner has taken the shard, and that reader stops at once. A fully consumed
shard is checkpointed as [`SHARD_END`], which is the signal its children may start.

The default store is [`MemoryLeaseStore`]: in process, correct for a single service instance, and
empty again after a restart. The `dynamodb-lease` feature adds [`DynamoLeaseStore`], and several
instances of a service then share the shards. The table needs a string partition key named
`lease_key` and nothing else; on-demand billing is enough. Every write is conditional and bumps a
fencing counter, so two instances cannot both hold one shard.

```
# #[cfg(feature = "dynamodb-lease")]
# mod demo {
use std::error::Error;
use std::sync::Arc;

use aws_config::BehaviorVersion;
use ruststream_kinesis::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(KinesisStream::new("orders"))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

// The lease store needs the AWS config too, so the service resolves it and hands the broker the
// same one instead of letting it build its own.
pub async fn run() -> Result<(), Box<dyn Error>> {
    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let leases = Arc::new(DynamoLeaseStore::new(&config, "orders-leases"));
    let broker = KinesisBroker::from_config(config)
        .lease_store(leases)
        // Names this instance in the lease table; a process-unique value is used when left out.
        .owner_id("instance-a");

    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(broker, |b| {
            b.include(handle);
        })
        .run()
        .await?;
    Ok(())
}
# }
# fn main() {}
```

## Delayed redelivery and its cap

Kinesis holds no redelivery timer, no delivery counter and no dead-letter mechanism, so nothing
here is native: the descriptor answers `Copies = AddressedCopies`, and the framework publishes
every copy itself. `HandlerOutcome::retry_after(delay)` becomes a copy of the record published once
the delay is over, with the attempt count in a header.

The copy goes back to the stream the subscription reads, which is the address the descriptor
reports, so a registration owes no destination of its own. `.max_attempts(n)` is how many
deliveries one record gets, counting the first, and `.dead_letter(name)` is the stream a spent
record leaves for, as it arrived, payload and headers. A cap declared without a dead letter
checkpoints past the spent record instead, which is what `drop()` does by hand.

```
# mod demo {
use std::time::Duration;

use ruststream_kinesis::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

/// Asks for a pause when the order cannot be settled yet, and counts nothing: how many pauses it
/// gets is the mount site's to say.
#[subscriber(KinesisStream::new("orders"))]
async fn reconcile(order: &Order) -> HandlerOutcome {
    if order.id.is_multiple_of(2) {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry_after(Duration::from_secs(30))
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        // Four deliveries per order, then the record leaves for a stream an operator reads.
        b.include(reconcile)
            .max_attempts(nonzero!(4))
            .dead_letter("orders.unsettled");
    })
}
# }
# fn main() {}
```

Under a declared cap `HandlerOutcome::retry()` becomes a published copy as well: only a copy
carries the count forward, and a record left on the shard would be read again with the count it
started with.

The copy arrives at the tip of the stream, not in the place the record held, and its partition key
picks the shard it lands on, so a deferred record loses its order against the records it was
published among. Where that order matters, declare no cap and let `HandlerOutcome::retry()` hold
the shard: the watermark stops at the record, and the shard replays from it.

`.out_retry(policy)` replaces the publisher those copies leave through rather than enabling them.
The position is an ordinary `Out` slot, so the chain then takes `.codec(..)`, `.transform(..)` and
this crate's [`partition_key(..)`](KinesisPublishSettings::partition_key). The copy carries the
record's own bytes, so the codec resolves the position and encodes nothing.

## Batches

A handler that takes a slice consumes a batch, and `.batch(n)` at the mount site names the size.
Here it becomes the `GetRecords` limit every shard reader asks with, so one read fetches at most
one batch's worth and no batch carries more records than it named:

```
# mod demo {
use std::time::Duration;

use ruststream_kinesis::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(KinesisStream::new("orders"))]
async fn digest(batch: &[Order]) -> HandlerOutcome {
    for order in batch {
        println!("order {}", order.id);
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        // The framework's word first, this crate's read settings after it.
        b.include(
            digest
                .batch(nonzero!(500))
                .poll_interval(Duration::from_millis(500)),
        );
    })
}
# }
# fn main() {}
```

A batch carries fewer records when that is all the shards had: one still filling is delivered 50 ms
after its first record. One batch mixes records from every shard this instance owns, and each
record settles on its own against its shard's watermark, so a body that returns one outcome per
element checkpoints per record.

The size is a cost decision too: a small batch means small reads against the same five reads a
second per shard, and [`poll_interval`](KinesisStream::poll_interval) is where that budget is
spent. A read the service throttles is not a delivery failure - the reader waits one interval and
reads again from where it stopped, so a handler sees a pause rather than an error.

## Positions and seeking

[`KinesisPosition`] is the whole vocabulary for where a subscription reads from. By default each
shard resumes from its stored checkpoint, and a shard without one opens at the tip.

| Position | Scope | Meaning |
| --- | --- | --- |
| [`KinesisPosition::horizon()`](KinesisPosition::horizon) | Stream-wide | The trim horizon: everything the stream still retains. |
| [`KinesisPosition::latest()`](KinesisPosition::latest) | Stream-wide | The tip: only records published after the reposition. |
| [`KinesisPosition::timestamp(millis)`](KinesisPosition::timestamp) | Stream-wide | Each shard opens at its first record from that instant, in milliseconds since the Unix epoch. |
| [`KinesisPosition::sequence(shard, seq)`](KinesisPosition::sequence) | One shard | Exactly one record. |

A stream-wide position reaches shards discovered later too, the children of a split among them, so
a seek keeps its meaning when the stream reshards. The shard-scoped form is the pinned position the
framework captures from a delivered record (`Positioned::position`): seeking to it redelivers that
record and moves no other shard. It needs a live reader for that shard in this instance, and errors
for a shard this instance does not own or has already finished.

`start_at(..)` opens the subscription at a position you name, ahead of any stored checkpoint. A
running subscription repositions from a handler: this broker fills the delivery context with the
record's position and the subscription's seeker, and [`Ctx<SeekHandle>`](SeekHandle) binds that
seeker as a handler parameter.

```
# mod demo {
use ruststream_kinesis::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Job {
    id: u64,
}

/// Replays the retained backlog; on the marker record it abandons the rest of it and follows the
/// tip instead.
#[subscriber(KinesisStream::new("jobs"), start_at(KinesisPosition::horizon()))]
async fn replay(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    if job.id == 999 && seeker.seek(KinesisPosition::latest()).await.is_err() {
        return HandlerOutcome::retry();
    }
    println!("replayed job {}", job.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(replay);
    })
}
# }
# fn main() {}
```

A reposition drops the watermark bookkeeping of every shard it moves, so an acknowledgement of a
record delivered before the seek cannot pull the cursor back over the position just taken. Records
from the new position onward are delivered again, which at-least-once permits.

## The delivery context

[`KinesisContext`] is what a single delivery carries, and it has two keys:
[`Position`] reads the record's own pinned [`KinesisPosition`], and [`SeekHandle`] reads the
subscription's [`KinesisSeeker`]. `Ctx(value): Ctx<Key>` binds one of them; a handler that wants
both names `KinesisContext` as its context type and reads the keys with `ctx.context(..)`.

A batch handler gets [`KinesisBatchContext`]. It carries the same [`SeekHandle`], because a seek
moves the whole subscription, and no position, because a batch spans many records; a body that
reacts to a position reads the [`SEQUENCE_HEADER`] and [`SHARD_HEADER`] headers of its elements.

Every delivery carries three headers: `kinesis-sequence-number` ([`SEQUENCE_HEADER`]),
`kinesis-shard-id` ([`SHARD_HEADER`]) and `partition-key` ([`PARTITION_KEY_HEADER`], where
`IncomingMessage::partition_key` and the framework's per-key dispatch find the key).

# Publishing

[`KinesisPublish`] is the publish policy, named [`Publish`](prelude::Publish) in the prelude. It is
pure declaration, constructible anywhere, and the runtime pairs it into a [`KinesisPublisher`] on
the connected broker. It is also this broker's default policy, so a replying handler mounted
without an explicit policy replies through it.

- `.out_reply(Publish::default())` names it for a reply.
- `.out(marker, Publish::default()).build()` binds the publisher of an injected slot.
- `.out_retry(Publish::default())` names it for the deferred retry copy.

The publish builder is the framework's - `message(..)`, `to(..)` with the stream name or ARN,
`with_headers(..)`, `publish()` - and this crate adds one step to it. There is no `RequestReply`
here: Kinesis has no reply address and no correlation primitive, so a request/reply exchange would
be a second stream the crate would have to invent. There are no transactions either, neither owned
nor borrowed: a publish is one `PutRecord` call and the service has nothing to commit.

## The partition key

The partition key picks the shard a record lands on, and with it the order that record keeps
against its neighbours. It belongs to one record rather than to a publisher, so it is a step on the
publish, [`partition_key(..)`](KinesisPublishSteps::partition_key), standing among the framework's
own steps. Per-message settings are the one thing a handler body says a broker's name for: a body
that names the key imports this crate's prelude and bounds its slot on
[`KinesisPublishOptions`].

```
# mod demo {
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[derive(Serialize, Outgoing)]
struct Journalled {
    order: u64,
}

/// The slot the journal entries leave through.
#[derive(OutSlot)]
#[publishes(Journalled)]
struct Journal;

/// The bound on the options type is what puts the step within reach, and it says in the signature
/// that this body is written for Kinesis.
#[subscriber(KinesisStream::new("orders"))]
async fn record(
    order: &Order,
    Out(journal): Out<impl Publisher<Options = KinesisPublishOptions>, Journal>,
) -> HandlerOutcome {
    let sent = journal
        .message(&Journalled { order: order.id })
        .to("orders.journal")
        .partition_key("tenant-acme")
        .publish()
        .await;
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(record).out(Journal, Publish::default()).build();
    })
}
# }
# fn main() {}
```

The step is a position on the builder and not a publisher wrapped around another, so the record
still leaves through the entry the mount site named: that entry's codec still encodes it, the
transforms on it still run, and a test still attributes the publish to that entry's slot. A body
that names no key keeps the framework's prelude and the plain `Out<impl Publisher, _>` bound, and
moves between brokers untouched.

Four places can name the key, and they resolve in one order:

1. The [`partition_key(..)`](KinesisPublishSteps::partition_key) step of this publish.
2. The `partition-key` header the call wrote by hand - the portable spelling, and what a record
   carries when it arrives from another broker of this framework.
3. The key the mount site named on the policy.
4. A process-unique key, which spreads the records: a Kinesis record cannot go out without one.

The key travels in the record's own partition-key field, and the header is its portable name at
both ends: the publisher writes the resolved key into that field and leaves the header out of the
envelope, and a delivery reports the key back under that header.

## Replies

A reply has no call site to put a step on: a replying handler returns a value and the runtime
publishes it. Its key is named on the policy instead, at the mount site, through
[`KinesisPublishSettings`]:

```
# mod demo {
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u64,
}

/// A receipt always goes to the receipts stream, so the type is where that stream is named.
#[derive(Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

#[subscriber(KinesisStream::new("orders"), publish)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        // One key means one shard: every receipt is ordered against every other, at the cost of
        // a single shard's throughput. Leave it off to spread them instead.
        b.include(confirm)
            .out_reply(Publish::default())
            .partition_key("receipts-v1");
    })
}
# }
# fn main() {}
```

## The header envelope

A Kinesis record carries a data blob and a partition key and nothing else, so user headers beyond
the partition key are written into a small envelope around the payload, and only when such headers
exist. A record published with no user headers is the plain payload, which any Kinesis consumer
reads, and a record written by another producer is read back as headerless. Otherwise the blob is
the four-byte magic `RSK1`, a big-endian `u32` header-block length, the header block, and the
payload.

# The prelude

`use ruststream_kinesis::prelude::*;` is the one glob a mounting file writes: the framework's own
prelude, plus [`KinesisBroker`], [`KinesisStream`] with [`KinesisSubscriberExt`],
[`KinesisPosition`] with `Seeker` and `Positioned`, the delivery contexts with the [`Position`] and
[`SeekHandle`] keys, [`DynamoLeaseStore`] under its feature, and the publish surface -
[`KinesisPublish`] under the uniform name [`Publish`](prelude::Publish), plus
[`KinesisPublishSettings`], [`KinesisPublishSteps`] and [`KinesisPublishOptions`]. Each prefixed
original stays at the crate root, for a file that mounts two brokers and has to say which `Publish`
it means.

A handler body imports the framework alone (`use ruststream::prelude::*`) and bounds an injected
publisher with a capability trait, so it never learns which broker it runs on. The one exception is
the body that names a per-message setting: it imports this glob for
[`partition_key`](KinesisPublishSteps::partition_key) and bounds its slot as
`Out<impl Publisher<Options = KinesisPublishOptions>, Marker>`.

# The generated document

`DescribeServer` puts the broker in the `servers` section under the `kinesis` protocol. What it
reports is a coordinate - the host and port a client dials, never credentials:

- With [`endpoint(..)`](KinesisBroker::endpoint), the host and port of that URL; the scheme and any
  user name and password come off it, because the document is generated to be published.
- Otherwise `kinesis.<region>.amazonaws.com` when [`region(..)`](KinesisBroker::region) named one.
- Otherwise `kinesis.amazonaws.com`, because a broker resolving its region from the environment has
  not resolved it when the document is built.

The `asyncapi` feature adds what only this broker knows, under the extension key
`x-ruststream-kinesis`: the specification lists no Kinesis binding, and the protocol keys it does
define are a closed list. A subscription describes its channel with the stream it reads, the pause
between reads, and the shards it provisions where it creates the stream. A publishing position
describes its own channel with the stream the records land in - the policy holds no destination, so
that stream is the one the mount site resolved. A policy that fixed a partition key describes the
records that leave through it, on the message object, since a key routes one record and never names
a stream.

```
# #[cfg(feature = "asyncapi")]
# mod demo {
use ruststream::asyncapi::{Spec, build_spec};
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JsonSchema)]
struct Order {
    id: u64,
}

#[derive(Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

#[subscriber(KinesisStream::new("orders").create_if_missing(2), publish)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

pub fn document() -> Spec {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisBroker::new(),
        |b| {
            b.include(confirm)
                .out_reply(Publish::default())
                .partition_key("tenant-acme");
        },
    );
    build_spec(&app)
}
# }
# fn main() {
#     #[cfg(feature = "asyncapi")]
#     let _ = demo::document();
# }
```

Only what the descriptor and the policy hold is reported. The document is built before anything
connects, so an existing stream's real shard count and its ARN are not in it, and neither is a key
named on a single publish call. There is no reply address either, because this broker has none.

# Testing

The `testing` feature ships [`KinesisTestBroker`](testing::KinesisTestBroker): an in-process
transport with no server and no network, driving the framework's `TestApp` harness. The harness
itself is the core's, and its own overview is the place to learn it:
<https://docs.rs/ruststream/latest/ruststream/testing/index.html>.

The mount is the service's own on both sides: [`KinesisStream`] opens the subscription here with
the settings it carries in production, and [`KinesisPublish`] pairs into the stand-in's publisher.
There is no test-only descriptor and no test-only policy.

```
# #[cfg(feature = "testing")]
# mod demo {
use ruststream::testing::TestApp;
use ruststream_kinesis::prelude::*;
use ruststream_kinesis::testing::KinesisTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
pub struct Order {
    pub id: u64,
}

#[subscriber(KinesisStream::new("orders"))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

pub async fn run() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(KinesisTestBroker::new(), |b| {
            b.include(handle);
        });
    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<KinesisTestBroker>()
        .message(&Order { id: 7 })
        .to("orders")
        .publish()
        .await
        .expect("the publish succeeds");
    tb.settle().await.expect("the delivery settles");
    tb.broker::<KinesisTestBroker>()
        .subscriber("orders")
        .assert_called(1)
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("the harness shuts down");
}
# }
# fn main() {
#     #[cfg(feature = "testing")]
#     tokio::runtime::Builder::new_multi_thread()
#         .enable_all()
#         .build()
#         .expect("the runtime starts")
#         .block_on(demo::run());
# }
```

The transport keeps a retained log per stream, because that is the property a handler observes
without a server: a subscription opens at the tip, and `start_at(..)` or a handler's
[`SeekHandle`] really re-reads the log from the position it names. A delivery carries the full
surface - a [`KinesisPosition`], the three headers, the partition key, and the context the keys
read - and a keyed publish resolves through the same four-step ladder the service uses, so it reads
back here the way it will read back live. A per-message setting is recorded against the slot the
publish left through, so the harness asserts it directly.

What the stand-in does not have is what a server owns. It routes one shard
([`IN_PROCESS_SHARD`](testing::IN_PROCESS_SHARD)), so there are no leases, no checkpoint
durability, no retention limits, no resharding and no redelivery timing. Those are covered by the
crate's live suites against a real stack.

# Operations

- Credentials and region resolve from the environment through the AWS default chain when the
  runtime connects. [`from_config`](KinesisBroker::from_config) takes a config the service built
  itself, which is also what a shared [`DynamoLeaseStore`] needs.
- TLS comes from the SDK's default HTTPS client; the crate adds no transport configuration of its
  own. [`test_credentials`](KinesisBroker::test_credentials) supplies dummy static credentials for
  a local stack that wants them present and ignores their values.
- A local stack is three settings: `endpoint`, `test_credentials` and `region`. `just brokers-up`
  starts the one this repository's compose file defines, and `just test-brokers` runs the live
  suites against it.
- Reads are billed per shard: five a second, shared with every other consumer of that stream.
  [`poll_interval`](KinesisStream::poll_interval) and the batch size are where that budget is
  spent.
- Enhanced fan-out is not implemented, so a subscription competes for the stream's shared
  throughput with every other reader.
- KPL-aggregated records are rejected with [`KinesisError::AggregatedRecord`]; this crate does not
  deaggregate them.
- One publish is one `PutRecord` call. The crate does not group records into `PutRecords`, so the
  client-side batching a high-rate producer wants is not here.
- [`KinesisError`] is the crate's one error enum, variants by source; the SDK's layered error types
  are boxed and formatted with their cause chain rather than leaked.

# Cargo features

Strictly additive, and all three are off by default.

- `asyncapi`: the `x-ruststream-kinesis` binding objects this crate writes into the generated
  document.
- `dynamodb-lease`: [`DynamoLeaseStore`], so several service instances share the shards.
- `testing`: the in-process [`KinesisTestBroker`](testing::KinesisTestBroker) and its `TestApp`
  support.

Examples for every topic above are in
[`examples/`](https://github.com/powersemmi/ruststream-kinesis/tree/main/crates/ruststream-kinesis/examples).
