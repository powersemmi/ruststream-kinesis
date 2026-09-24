//! Two streams consumed by one broker, gated behind `KINESIS_TEST_ENDPOINT`.
//!
//! Shard ids repeat across streams: the first shard of every stream is `shardId-000000000000`. A
//! broker consuming two streams keeps both in one lease store, so the store has to tell the
//! streams apart, or one stream's progress opens the other stream's shard and a shard finished on
//! one stream retires the other's. Only a real stream has shards to repeat, so both claims need a
//! stack.
//!
//! Start one with `just brokers-up`, then:
//! `KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_kinesis::client::Waiters;
use aws_sdk_kinesis::types::Shard;
use futures::StreamExt;

use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, StartAt, Subscriber,
    SubscriptionSource,
};
use ruststream_kinesis::{
    ConnectedKinesisBroker, KinesisBroker, KinesisMessage, KinesisPosition, KinesisStream,
    KinesisSubscriber, LeaseStore, MemoryLeaseStore, SHARD_HEADER,
};

mod live;

const RECV_TIMEOUT: Duration = Duration::from_secs(30);
/// The owner id both brokers of a test share, so the second one takes the shards the first one
/// held without waiting for the leases to lapse.
const OWNER: &str = "instance-a";

/// The stack's endpoint, or `None` to skip the live checks below. Under `RUSTSTREAM_REQUIRE_LIVE`
/// a missing endpoint is a failure instead, so a job that started a stack cannot report `ok`
/// without having reached it.
fn test_endpoint() -> Option<String> {
    live::url("KINESIS_TEST_ENDPOINT")
}

/// Connects with a lease store the test keeps, so a second broker can resume from what the first
/// one recorded: the store a restarted service reads its progress back from.
async fn connect_with(endpoint: &str, leases: &Arc<MemoryLeaseStore>) -> ConnectedKinesisBroker {
    KinesisBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
        .lease_store(Arc::clone(leases) as Arc<dyn LeaseStore>)
        .owner_id(OWNER)
        .connect()
        .await
        .expect("broker connects")
}

/// Per-test unique stream, so runs do not observe each other's leftovers.
fn unique(name: &str) -> String {
    format!("st-{name}-{}", std::process::id())
}

/// A client onto the same stack, for splitting a shard under a running subscription.
async fn observer(endpoint: &str) -> aws_sdk_kinesis::Client {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .test_credentials()
        .load()
        .await;
    aws_sdk_kinesis::Client::new(&config)
}

/// The descriptor with a short pause between reads, so a live test is not paced by the service's
/// one-second recommendation.
fn source(stream: &str) -> KinesisStream {
    KinesisStream::new(stream).poll_interval(Duration::from_millis(200))
}

/// A one-shard stream opened at the trim horizon, created when missing.
async fn from_horizon(connected: &ConnectedKinesisBroker, stream: &str) -> KinesisSubscriber {
    StartAt::new(
        source(stream).create_if_missing(1),
        KinesisPosition::horizon(),
    )
    .subscribe(connected)
    .await
    .expect("subscription opens")
}

async fn publish(connected: &ConnectedKinesisBroker, stream: &str, payload: &str) {
    connected
        .publisher()
        .publish(OutgoingMessage::new(stream, payload.as_bytes()), None)
        .await
        .expect("publish succeeds");
}

/// The next delivery of `subscriber`, which must be `expected`.
async fn receive(subscriber: &mut KinesisSubscriber, expected: &str) -> KinesisMessage {
    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("`{expected}` arrives"))
        .expect("stream is open")
        .unwrap_or_else(|err| panic!("`{expected}` is delivered, not an error: {err}"));
    assert_eq!(
        String::from_utf8_lossy(message.payload()),
        expected,
        "the stream delivers its own records in order",
    );
    message
}

/// Splits the stream's only shard and waits until the stream is usable again.
async fn split(client: &aws_sdk_kinesis::Client, stream: &str, parent: &str) {
    let shards = client
        .list_shards()
        .stream_name(stream)
        .send()
        .await
        .expect("the stack lists the shards")
        .shards()
        .to_vec();
    let range = shards
        .iter()
        .find(|shard| shard.shard_id() == parent)
        .and_then(Shard::hash_key_range)
        .expect("the parent shard reports its hash key range");
    let low: u128 = range
        .starting_hash_key()
        .parse()
        .expect("a hash key is a 128-bit decimal");
    let high: u128 = range
        .ending_hash_key()
        .parse()
        .expect("a hash key is a 128-bit decimal");
    client
        .split_shard()
        .stream_name(stream)
        .shard_to_split(parent)
        .new_starting_hash_key((low + (high - low) / 2).to_string())
        .send()
        .await
        .expect("the stack splits the shard");
    client
        .wait_until_stream_exists()
        .stream_name(stream)
        .wait(Duration::from_secs(60))
        .await
        .expect("the stream becomes usable again");
}

/// A restarted service resumes each stream from that stream's own progress. The two streams
/// checkpoint the same shard id, the second one last, and the first stream must still resume
/// right after its own acknowledged record: not from the other stream's sequence number, which
/// either fails to open the shard or lands somewhere in it and skips records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_stream_resumes_from_its_own_checkpoint() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let leases = Arc::new(MemoryLeaseStore::new());
    let first = unique("resume-first");
    let second = unique("resume-second");

    let connected = connect_with(&endpoint, &leases).await;
    let mut first_subscriber = from_horizon(&connected, &first).await;
    publish(&connected, &first, "first-1").await;
    let handled = receive(&mut first_subscriber, "first-1").await;
    let first_shard = handled
        .headers()
        .get_str(SHARD_HEADER)
        .expect("every delivery names its shard")
        .to_owned();
    handled.ack().await.expect("ack succeeds");

    // The second stream records progress on a shard with the same id, after the first one did.
    let mut second_subscriber = from_horizon(&connected, &second).await;
    for payload in ["second-1", "second-2", "second-3"] {
        publish(&connected, &second, payload).await;
    }
    for payload in ["second-1", "second-2", "second-3"] {
        let handled = receive(&mut second_subscriber, payload).await;
        assert_eq!(
            handled.headers().get_str(SHARD_HEADER),
            Some(first_shard.as_str()),
            "the two streams must share a shard id for this test to mean anything",
        );
        handled.ack().await.expect("ack succeeds");
    }
    drop(first_subscriber);
    drop(second_subscriber);
    connected.shutdown().await.expect("shutdown succeeds");

    // The service restarts over the same store; the first stream got a record while it was down.
    let connected = connect_with(&endpoint, &leases).await;
    publish(&connected, &first, "first-2").await;
    let mut resumed = source(&first)
        .subscribe(&connected)
        .await
        .expect("subscription opens");
    let next = receive(&mut resumed, "first-2").await;
    next.ack().await.expect("ack succeeds");
    drop(resumed);
    connected.shutdown().await.expect("shutdown succeeds");
}

/// A shard finished on one stream is finished there only. The first stream's only shard is split
/// and read to its end, and a second stream whose first shard carries the same id must still be
/// read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shard_finished_on_one_stream_leaves_the_other_stream_readable() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let client = observer(&endpoint).await;
    let leases = Arc::new(MemoryLeaseStore::new());
    let finished = unique("finished");
    let other = unique("other");

    let connected = connect_with(&endpoint, &leases).await;
    let mut finished_subscriber = from_horizon(&connected, &finished).await;
    publish(&connected, &finished, "before-the-split").await;
    let before = receive(&mut finished_subscriber, "before-the-split").await;
    let parent = before
        .headers()
        .get_str(SHARD_HEADER)
        .expect("every delivery names its shard")
        .to_owned();
    before.ack().await.expect("ack succeeds");

    split(&client, &finished, &parent).await;
    // The children start only once the parent is marked finished, so a record read from a child
    // proves the parent's end is in the store.
    publish(&connected, &finished, "after-the-split").await;
    let after = receive(&mut finished_subscriber, "after-the-split").await;
    assert_ne!(
        after.headers().get_str(SHARD_HEADER),
        Some(parent.as_str()),
        "a record published after the split arrives on a child",
    );
    after.ack().await.expect("ack succeeds");

    let mut other_subscriber = from_horizon(&connected, &other).await;
    publish(&connected, &other, "other-1").await;
    let delivered = receive(&mut other_subscriber, "other-1").await;
    assert_eq!(
        delivered.headers().get_str(SHARD_HEADER),
        Some(parent.as_str()),
        "the other stream's record lands on a shard with the finished shard's id",
    );
    delivered.ack().await.expect("ack succeeds");

    drop(finished_subscriber);
    drop(other_subscriber);
    connected.shutdown().await.expect("shutdown succeeds");
}
