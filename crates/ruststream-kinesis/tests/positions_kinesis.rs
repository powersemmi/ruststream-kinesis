//! The stream-wide start vocabulary against a real stream, gated behind `KINESIS_TEST_ENDPOINT`.
//!
//! [`KinesisPosition`] has four forms, and only the shard-scoped one is exercised by the
//! framework's `Seekable` suite in `conformance_kinesis.rs`. The other three become shard
//! iterator types, which is a mapping only the service can confirm: a stream-wide position is
//! meaningful only when the records it skips or replays were published before the subscription
//! existed, and no in-process log can tell a trim horizon from a tip the way a stored stream
//! does.
//!
//! Start one with `just brokers-up`, then:
//! `KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_kinesis::client::Waiters;
use aws_sdk_kinesis::types::ShardIteratorType;
use futures::StreamExt;

use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Seekable, Seeker,
    StartAt, Subscriber, SubscriptionSource,
};
use ruststream_kinesis::{
    ConnectedKinesisBroker, KinesisBroker, KinesisError, KinesisPosition, KinesisStream,
};

mod live;

const RECV_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a subscription that must stay silent is watched. Long enough that a reader which had
/// opened at the wrong position would have delivered the backlog by now.
const QUIET: Duration = Duration::from_secs(3);

/// The stack's endpoint, or `None` to skip the live checks below. Under `RUSTSTREAM_REQUIRE_LIVE`
/// a missing endpoint is a failure instead, so a job that started a stack cannot report `ok`
/// without having reached it.
fn test_endpoint() -> Option<String> {
    live::url("KINESIS_TEST_ENDPOINT")
}

fn broker(endpoint: &str) -> KinesisBroker {
    KinesisBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

async fn connect(endpoint: &str) -> ConnectedKinesisBroker {
    broker(endpoint).connect().await.expect("broker connects")
}

/// Per-test unique stream, so runs do not observe each other's leftovers.
fn unique(name: &str) -> String {
    format!("pos-{name}-{}", std::process::id())
}

/// A client onto the same stack, for the state the crate's own surface does not report: the
/// stream a test seeds before any subscription exists, and the arrival timestamps the service
/// stamps on the records it stored.
async fn observer(endpoint: &str) -> aws_sdk_kinesis::Client {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .test_credentials()
        .load()
        .await;
    aws_sdk_kinesis::Client::new(&config)
}

/// Creates the stream and waits until it is usable.
///
/// The tests below publish before any subscription exists, which is the only way to tell a start
/// position from the tip, so the stream cannot be the one a descriptor creates on subscribe.
async fn provision(client: &aws_sdk_kinesis::Client, stream: &str, shards: i32) {
    client
        .create_stream()
        .stream_name(stream)
        .shard_count(shards)
        .send()
        .await
        .expect("the stack creates the stream");
    client
        .wait_until_stream_exists()
        .stream_name(stream)
        .wait(Duration::from_secs(60))
        .await
        .expect("the stream becomes usable");
}

/// The arrival timestamps the service stamped on the stream's retained records, oldest first.
///
/// Waits until the stream holds `expected` of them, so the position a test names is the service's
/// own answer rather than the test's clock.
async fn arrival_millis(
    client: &aws_sdk_kinesis::Client,
    stream: &str,
    expected: usize,
) -> Vec<i64> {
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    let mut stamps = Vec::new();
    while tokio::time::Instant::now() < deadline {
        let shard = client
            .list_shards()
            .stream_name(stream)
            .send()
            .await
            .expect("the stack lists the shards")
            .shards()
            .first()
            .expect("a provisioned stream has a shard")
            .shard_id()
            .to_owned();
        let iterator = client
            .get_shard_iterator()
            .stream_name(stream)
            .shard_id(shard)
            .shard_iterator_type(ShardIteratorType::TrimHorizon)
            .send()
            .await
            .expect("the stack answers an iterator")
            .shard_iterator()
            .expect("a live shard has an iterator")
            .to_owned();
        stamps = client
            .get_records()
            .shard_iterator(iterator)
            .send()
            .await
            .expect("the stack answers the retained records")
            .records()
            .iter()
            .map(|record| {
                record
                    .approximate_arrival_timestamp()
                    .expect("the service stamps every record")
                    .to_millis()
                    .expect("an arrival timestamp fits in milliseconds")
            })
            .collect();
        if stamps.len() >= expected {
            return stamps;
        }
    }
    panic!(
        "the stream held {} records, expected {expected}",
        stamps.len()
    );
}

/// Waits until the wall clock has left the second `millis` falls in.
///
/// The stack's `AtTimestamp` iterator compares whole seconds, so two records published inside one
/// second cannot be told apart by an instant. The wait is part of the fixture rather than
/// synchronisation: it is what gives the position something to cut between.
async fn past_the_second_of(millis: i64) {
    let next_second = (millis / 1000 + 1) * 1000;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_millis();
    let now = i64::try_from(now).expect("the epoch millis fit in an i64");
    if let Ok(remaining) = u64::try_from(next_second - now + 1) {
        tokio::time::sleep(Duration::from_millis(remaining)).await;
    }
}

/// The trim horizon is the whole retained log, and a subscription opened there replays records
/// published long before it existed. Every other live suite publishes after subscribing, where a
/// horizon and a tip look the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_trim_horizon_replays_a_backlog_the_subscription_never_saw() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let client = observer(&endpoint).await;
    let stream_name = unique("horizon");
    provision(&client, &stream_name, 1).await;

    let connected = connect(&endpoint).await;
    let publisher = connected.publisher();
    for payload in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        publisher
            .publish(OutgoingMessage::new(&stream_name, payload), None)
            .await
            .expect("publish succeeds");
    }

    let mut subscriber = StartAt::new(
        KinesisStream::new(stream_name.as_str()).poll_interval(Duration::from_millis(200)),
        KinesisPosition::horizon(),
    )
    .subscribe(&connected)
    .await
    .expect("subscription opens");

    let mut stream = pin!(subscriber.stream());
    for expected in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the backlog arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(
            message.payload(),
            expected,
            "the horizon must replay the retained records in publish order",
        );
        message.ack().await.expect("ack succeeds");
    }

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The tip is the other half of the same fact: a subscription opened there ignores everything the
/// stream already holds and starts with the next record published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_tip_skips_the_backlog_and_takes_only_what_follows() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let client = observer(&endpoint).await;
    let stream_name = unique("latest");
    provision(&client, &stream_name, 1).await;

    let connected = connect(&endpoint).await;
    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::new(&stream_name, b"backlog".as_slice()),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut subscriber = StartAt::new(
        KinesisStream::new(stream_name.as_str()).poll_interval(Duration::from_millis(200)),
        KinesisPosition::latest(),
    )
    .subscribe(&connected)
    .await
    .expect("subscription opens");

    let mut stream = pin!(subscriber.stream());
    if let Ok(item) = tokio::time::timeout(QUIET, stream.next()).await {
        panic!("the tip must not replay the stored record, got {item:?}");
    }

    publisher
        .publish(
            OutgoingMessage::new(&stream_name, b"fresh".as_slice()),
            None,
        )
        .await
        .expect("publish succeeds");
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(
        message.payload(),
        b"fresh",
        "a subscription at the tip must deliver what is published after it opened",
    );
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A timestamp position opens each shard at its first record from that instant. The instant is the
/// service's own arrival stamp, read back from the stack, so the test names a position the records
/// really carry instead of one its own clock guessed. The stack compares those stamps by the whole
/// second, which is what the two batches are spaced for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timestamp_opens_the_subscription_at_the_instant_it_names() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let client = observer(&endpoint).await;
    let stream_name = unique("timestamp");
    provision(&client, &stream_name, 1).await;

    let connected = connect(&endpoint).await;
    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::new(&stream_name, b"earlier".as_slice()),
            None,
        )
        .await
        .expect("publish succeeds");
    let first = arrival_millis(&client, &stream_name, 1).await;
    past_the_second_of(first[0]).await;
    publisher
        .publish(
            OutgoingMessage::new(&stream_name, b"later".as_slice()),
            None,
        )
        .await
        .expect("publish succeeds");
    let stamps = arrival_millis(&client, &stream_name, 2).await;
    assert!(
        stamps[1] / 1000 > stamps[0] / 1000,
        "the two publishes must land in different seconds for the instant to separate them",
    );

    let at = u64::try_from(stamps[1]).expect("an arrival timestamp is after the epoch");
    let mut subscriber = StartAt::new(
        KinesisStream::new(stream_name.as_str()).poll_interval(Duration::from_millis(200)),
        KinesisPosition::timestamp(at),
    )
    .subscribe(&connected)
    .await
    .expect("subscription opens");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(
        message.payload(),
        b"later",
        "a timestamp position must skip the records the service stamped before it",
    );
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The shard-scoped position addresses one shard of this instance, and the crate says so: a
/// position naming a shard no reader here owns is refused rather than silently ignored. The
/// subscription survives the refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_position_on_a_shard_this_instance_does_not_own_is_refused() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let client = observer(&endpoint).await;
    let stream_name = unique("unowned");
    provision(&client, &stream_name, 1).await;

    let connected = connect(&endpoint).await;
    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::new(&stream_name, b"first".as_slice()),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut subscriber = StartAt::new(
        KinesisStream::new(stream_name.as_str()).poll_interval(Duration::from_millis(200)),
        KinesisPosition::horizon(),
    )
    .subscribe(&connected)
    .await
    .expect("subscription opens");
    // Minted before the stream borrows the subscriber, the way a handler's context carries it.
    let seeker = subscriber.seeker();

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"first");
    message.ack().await.expect("ack succeeds");

    let refused = seeker
        .seek(KinesisPosition::sequence("shardId-000000009999", "0"))
        .await
        .expect_err("a shard no reader here owns cannot be repositioned");
    assert!(
        matches!(refused, KinesisError::Read { .. }),
        "expected a read failure naming the shard, got {refused:?}",
    );

    publisher
        .publish(
            OutgoingMessage::new(&stream_name, b"second".as_slice()),
            None,
        )
        .await
        .expect("publish succeeds");
    let after = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(
        after.payload(),
        b"second",
        "a refused reposition must leave the subscription reading",
    );
    after.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}
