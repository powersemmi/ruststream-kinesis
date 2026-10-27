Amazon Kinesis Data Streams broker for the [RustStream](https://github.com/powersemmi/ruststream)
messaging framework, backed by the official [`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis)
client.

A subscription reads every shard of one stream, and a publish is one record on the stream the
outgoing message names. The crate adds what the vendor's consumer library gives other platforms:
shard discovery across splits and merges, a lease per shard, and per-shard checkpoints in a
[`LeaseStore`]. An ack is a checkpoint, one instance reads a shard at a time, and delivery is
at-least-once.

# A service

A handler is an `async fn` over the decoded record, mounted on a broker inside the application
object, and the attribute writes the `main`:

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
    tracing::info!(order.id, "accepted");
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

`cargo run -- run` starts it. [`KinesisBroker::new`] only records the configuration; the region
and the credentials resolve from the environment when the runtime connects the broker, and the
runtime shuts it down when the service stops.

# Where things are

The [guides][guides] take a Kinesis service from installation to production, in this order:
[getting started][g-start], [consuming a stream][g-consuming],
[shards, partition keys and ordering][g-ordering],
[checkpoints and delivery guarantees][g-checkpoints],
[retries and dead-letter streams][g-retries], [replay and seeking][g-replay],
[several instances][g-leases], [testing][g-testing] and
[running in production][g-production]. The [architecture][architecture] pages show how the crate
sits between the SDK client and the runtime, and the [benchmarks][benchmarks] what it costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Broker

- [KinesisBroker][c-broker]: [`KinesisBroker`], connected into [`ConnectedKinesisBroker`].
- [Credentials][c-credentials]: the AWS default chain at connect, or a config built elsewhere
  through [`KinesisBroker::from_config`].
- [Endpoint][c-endpoint]: [`KinesisBroker::endpoint`], [`KinesisBroker::region`] and
  [`KinesisBroker::test_credentials`], for a local stack.
- [Startup][c-startup]: a subscription opens once every shard it took has a reader.
- [Shutdown][c-shutdown]: a handle used after shutdown reports [`KinesisError::NotConnected`].
- [Prelude][c-prelude]: [`prelude`], the one glob a Kinesis service imports.
- [Errors][c-errors]: [`KinesisError`].

## Subscriptions

- [Subscriptions][c-subscriptions]: a bare stream name reads every shard of the stream.
- [KinesisStream][c-kinesis-stream]: [`KinesisStream`], one stream with every setting of its
  subscription.
- [poll_interval][c-poll-interval]: [`KinesisStream::poll_interval`], the pause between reads of
  an idle shard.
- [create_if_missing][c-create-if-missing]: [`KinesisStream::create_if_missing`], a stream created
  when it is missing.
- [KinesisSubscriberExt][c-subscriber-ext]: [`KinesisSubscriberExt`], the same settings on the
  mount chain.
- [Shards][c-shards]: a lease and a reader per shard, and a split's children after their parents.
- [Batches][c-batches]: a slice handler over [`KinesisSubscriber`] batches.

## Settlement

- [Checkpoint][c-settlement]: [`KinesisMessage`], what an ack, a drop and a requeue do on a
  shard.
- [retry_after][c-retry-after]: a deferred copy of the record back to its stream.
- [out_retry][c-out-retry]: the publisher the retry copies leave through.
- [Dead letters][c-dead-letters]: `max_attempts` and `dead_letter`, the stream a spent record
  goes to.

## Leases

- [LeaseStore][c-leases]: [`LeaseStore`], [`LeaseState`] and [`SHARD_END`], where shard owners
  and checkpoints live.
- [MemoryLeaseStore][c-memory-lease-store]: [`MemoryLeaseStore`], the default store for one
  instance.
- [DynamoLeaseStore][c-dynamo-lease-store]: [`DynamoLeaseStore`], a `DynamoDB` table several
  instances share (feature `dynamodb-lease`).
- [LeaseKey][c-lease-key]: [`LeaseKey`], one shard of one stream.
- [owner_id][c-owner-id]: [`KinesisBroker::owner_id`], the name this instance holds its leases
  under.
- [legacy_stream][c-legacy-stream]: [`DynamoLeaseStore::legacy_stream`], a lease table an
  earlier version wrote.

## Context

- [KinesisContext][c-context]: [`KinesisContext`] and its keys [`Position`] and [`SeekHandle`].
- [KinesisBatchContext][c-batch-context]: [`KinesisBatchContext`], a batch handler's context.
- [Headers][c-headers]: [`SEQUENCE_HEADER`], [`SHARD_HEADER`] and [`PARTITION_KEY_HEADER`] on
  every delivery.
- [Envelope][c-envelope]: how headers travel inside a record: [`Rsk`], [`Framed`], [`NoOp`] or
  an [`Envelope`] of the service's own, chosen on the broker, the subscription or the policy.

## Positions

- [KinesisPosition][c-positions]: [`KinesisPosition`], a place in the stream.
- [start_at][c-start-at]: opening a subscription at a position on every startup.
- [Seek][c-seek]: [`KinesisSeeker`] through the [`SeekHandle`] key, moving a running
  subscription.

## Publishing

- [KinesisPublish][c-publishing]: [`KinesisPublish`], the publish policy, and its live
  [`KinesisPublisher`].
- [Keys][c-keys]: the four places that name a record's partition key.
- [partition_key][c-partition-key]: [`KinesisPublishSteps::partition_key`] and
  [`KinesisPublishOptions`] key one record.
- [KinesisPublishSettings][c-publish-settings]: [`KinesisPublishSettings::partition_key`] keys
  every record of a position.

## Tooling

- [AsyncAPI][c-asyncapi]: the Kinesis bindings of the generated document (feature `asyncapi`):
  the [server][c-asyncapi-server], the [channels][c-asyncapi-channels] and the
  [messages][c-asyncapi-messages].
- [Testing][c-testing]: the in-process Kinesis
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  the broker to (feature `testing`), and [Live][c-live] runs against `LocalStack` or AWS.

# Cargo features

All three are additive and off by default. `asyncapi` contributes the Kinesis bindings to the
generated document, `dynamodb-lease` adds [`DynamoLeaseStore`], and `testing` adds the in-process
mode `TestApp::start` connects the broker through. The codecs are the core's: a service enables
`json`, `msgpack` or `cbor` on `ruststream`.

[guides]: https://powersemmi.github.io/ruststream-kinesis/latest/
[g-start]: https://powersemmi.github.io/ruststream-kinesis/latest/guides/getting-started/
[g-consuming]: https://powersemmi.github.io/ruststream-kinesis/latest/guides/consuming/
[g-ordering]: https://powersemmi.github.io/ruststream-kinesis/latest/guides/ordering/
[g-checkpoints]: https://powersemmi.github.io/ruststream-kinesis/latest/guides/checkpoints/
[g-retries]: https://powersemmi.github.io/ruststream-kinesis/latest/guides/retries/
[g-replay]: https://powersemmi.github.io/ruststream-kinesis/latest/guides/replay/
[g-leases]: https://powersemmi.github.io/ruststream-kinesis/latest/guides/leases/
[g-testing]: https://powersemmi.github.io/ruststream-kinesis/latest/guides/testing/
[g-production]: https://powersemmi.github.io/ruststream-kinesis/latest/guides/production/
[architecture]: https://powersemmi.github.io/ruststream-kinesis/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-kinesis/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-kinesis/latest/components/broker/
[c-credentials]: https://powersemmi.github.io/ruststream-kinesis/latest/components/broker/credentials/
[c-endpoint]: https://powersemmi.github.io/ruststream-kinesis/latest/components/broker/endpoint/
[c-startup]: https://powersemmi.github.io/ruststream-kinesis/latest/components/broker/startup/
[c-shutdown]: https://powersemmi.github.io/ruststream-kinesis/latest/components/broker/shutdown/
[c-prelude]: https://powersemmi.github.io/ruststream-kinesis/latest/components/broker/prelude/
[c-errors]: https://powersemmi.github.io/ruststream-kinesis/latest/components/broker/errors/
[c-subscriptions]: https://powersemmi.github.io/ruststream-kinesis/latest/components/subscriptions/
[c-kinesis-stream]: https://powersemmi.github.io/ruststream-kinesis/latest/components/subscriptions/kinesis-stream/
[c-poll-interval]: https://powersemmi.github.io/ruststream-kinesis/latest/components/subscriptions/poll-interval/
[c-create-if-missing]: https://powersemmi.github.io/ruststream-kinesis/latest/components/subscriptions/create-if-missing/
[c-subscriber-ext]: https://powersemmi.github.io/ruststream-kinesis/latest/components/subscriptions/subscriber-ext/
[c-shards]: https://powersemmi.github.io/ruststream-kinesis/latest/components/subscriptions/shards/
[c-batches]: https://powersemmi.github.io/ruststream-kinesis/latest/components/subscriptions/batches/
[c-settlement]: https://powersemmi.github.io/ruststream-kinesis/latest/components/settlement/
[c-retry-after]: https://powersemmi.github.io/ruststream-kinesis/latest/components/settlement/retry-after/
[c-out-retry]: https://powersemmi.github.io/ruststream-kinesis/latest/components/settlement/out-retry/
[c-dead-letters]: https://powersemmi.github.io/ruststream-kinesis/latest/components/settlement/dead-letters/
[c-leases]: https://powersemmi.github.io/ruststream-kinesis/latest/components/leases/
[c-memory-lease-store]: https://powersemmi.github.io/ruststream-kinesis/latest/components/leases/memory-lease-store/
[c-dynamo-lease-store]: https://powersemmi.github.io/ruststream-kinesis/latest/components/leases/dynamo-lease-store/
[c-lease-key]: https://powersemmi.github.io/ruststream-kinesis/latest/components/leases/lease-key/
[c-owner-id]: https://powersemmi.github.io/ruststream-kinesis/latest/components/leases/owner-id/
[c-legacy-stream]: https://powersemmi.github.io/ruststream-kinesis/latest/components/leases/legacy-stream/
[c-context]: https://powersemmi.github.io/ruststream-kinesis/latest/components/context/
[c-batch-context]: https://powersemmi.github.io/ruststream-kinesis/latest/components/context/batch-context/
[c-headers]: https://powersemmi.github.io/ruststream-kinesis/latest/components/context/headers/
[c-envelope]: https://powersemmi.github.io/ruststream-kinesis/latest/components/context/envelope/
[c-positions]: https://powersemmi.github.io/ruststream-kinesis/latest/components/positions/
[c-start-at]: https://powersemmi.github.io/ruststream-kinesis/latest/components/positions/start-at/
[c-seek]: https://powersemmi.github.io/ruststream-kinesis/latest/components/positions/seek/
[c-publishing]: https://powersemmi.github.io/ruststream-kinesis/latest/components/publishing/
[c-keys]: https://powersemmi.github.io/ruststream-kinesis/latest/components/publishing/keys/
[c-partition-key]: https://powersemmi.github.io/ruststream-kinesis/latest/components/publishing/partition-key/
[c-publish-settings]: https://powersemmi.github.io/ruststream-kinesis/latest/components/publishing/publish-settings/
[c-asyncapi]: https://powersemmi.github.io/ruststream-kinesis/latest/components/asyncapi/
[c-asyncapi-server]: https://powersemmi.github.io/ruststream-kinesis/latest/components/asyncapi/server/
[c-asyncapi-channels]: https://powersemmi.github.io/ruststream-kinesis/latest/components/asyncapi/channels/
[c-asyncapi-messages]: https://powersemmi.github.io/ruststream-kinesis/latest/components/asyncapi/messages/
[c-testing]: https://powersemmi.github.io/ruststream-kinesis/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-kinesis/latest/components/testing/live/
