//! The shard set of a real stream, gated behind `KINESIS_TEST_ENDPOINT`.
//!
//! This is what the crate adds on top of the SDK and what no in-process transport has: a stream
//! is cut into shards, the service picks a record's shard from its partition key, and the set
//! changes under a running subscription when the stream is resharded. The in-process stand-in
//! routes one shard on purpose, so every claim here needs a stack.
//!
//! Start one with `just brokers-up`, then:
//! `KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

use std::collections::{HashMap, HashSet};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_kinesis::client::Waiters;
use aws_sdk_kinesis::types::Shard;
use futures::StreamExt;

use ruststream::{
    Broker, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher, StartAt,
    Subscriber, SubscriptionSource,
};
use ruststream_kinesis::{
    ConnectedKinesisBroker, KinesisBroker, KinesisPosition, KinesisPublisher, KinesisStream,
    LeaseStore, MemoryLeaseStore, PARTITION_KEY_HEADER, SHARD_END, SHARD_HEADER,
};

mod live;

const RECV_TIMEOUT: Duration = Duration::from_secs(60);
/// Records published under one key, and distinct keys published beside them. Sixteen keys over
/// two shards cover both: the service hashes the key, so which shard each one takes is fixed.
const PINNED: usize = 8;
const SPREAD: usize = 16;

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

/// Connects with a lease store the test keeps a handle on, so it can read back what the readers
/// recorded about the shards they finished.
async fn connect_with(endpoint: &str, leases: &Arc<MemoryLeaseStore>) -> ConnectedKinesisBroker {
    broker(endpoint)
        .lease_store(Arc::clone(leases) as Arc<dyn LeaseStore>)
        .connect()
        .await
        .expect("broker connects")
}

/// Per-test unique stream, so runs do not observe each other's leftovers.
fn unique(name: &str) -> String {
    format!("sh-{name}-{}", std::process::id())
}

/// A client onto the same stack, for the shard set itself: what the service provisioned, which
/// shards a split produced, and which of them are closed.
async fn observer(endpoint: &str) -> aws_sdk_kinesis::Client {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .test_credentials()
        .load()
        .await;
    aws_sdk_kinesis::Client::new(&config)
}

/// The stream's shards as the service reports them.
async fn shards_of(client: &aws_sdk_kinesis::Client, stream: &str) -> Vec<Shard> {
    client
        .list_shards()
        .stream_name(stream)
        .send()
        .await
        .expect("the stack lists the shards")
        .shards()
        .to_vec()
}

/// Whether the service has closed this shard: a split or a merge ends its sequence range.
fn is_closed(shard: &Shard) -> bool {
    shard
        .sequence_number_range()
        .and_then(|range| range.ending_sequence_number())
        .is_some()
}

/// Publishes one record under `key`, the portable spelling of the partition key: the record's own
/// field carries it, and the service picks the shard from it.
async fn publish_keyed(publisher: &KinesisPublisher, stream: &str, key: &str, payload: &str) {
    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, key.to_owned());
    publisher
        .publish(
            OutgoingMessage::new(stream, payload.as_bytes()).with_headers(headers),
            None,
        )
        .await
        .expect("publish succeeds");
}

/// The descriptor with a short pause between reads, so a live test is not paced by the service's
/// one-second recommendation.
fn source(stream: &str) -> KinesisStream {
    KinesisStream::new(stream).poll_interval(Duration::from_millis(200))
}

/// The same descriptor, creating the stream with `shards` and opened at the trim horizon, so
/// every record the test publishes is read whether or not the reader was up when it landed.
fn from_horizon(stream: &str, shards: i32) -> StartAt<KinesisStream, KinesisPosition> {
    StartAt::new(
        source(stream).create_if_missing(shards),
        KinesisPosition::horizon(),
    )
}

/// Splits `parent` down the middle of its own hash key range and waits until the stream is usable
/// again, then holds the service to what a split means: the parent closed, and two children
/// naming it.
async fn split_in_half(client: &aws_sdk_kinesis::Client, stream: &str, parent: &str) {
    let shards = shards_of(client, stream).await;
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

    let after = shards_of(client, stream).await;
    assert_eq!(
        after.len(),
        3,
        "a split leaves the closed parent and its two children",
    );
    assert_eq!(
        after
            .iter()
            .filter(|shard| shard.parent_shard_id() == Some(parent))
            .count(),
        2,
        "both children must name the parent",
    );
    assert!(
        after
            .iter()
            .any(|shard| shard.shard_id() == parent && is_closed(shard)),
        "the parent must be closed once it is split",
    );
}

/// `create_if_missing(n)` is the one descriptor setting that changes the service's own state, and
/// the shard count it provisions is the service's answer, not the descriptor's. The second mount
/// is the other half: an existing stream is left as it is, whatever the descriptor would have
/// asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_if_missing_provisions_the_shard_count_it_names() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let client = observer(&endpoint).await;
    let connected = connect(&endpoint).await;
    let stream_name = unique("provision");

    let subscriber = source(&stream_name)
        .create_if_missing(3)
        .subscribe(&connected)
        .await
        .expect("subscription opens and creates the stream");
    let provisioned = shards_of(&client, &stream_name).await;
    assert_eq!(
        provisioned.len(),
        3,
        "the service must hold the shards the descriptor named",
    );
    drop(subscriber);

    let second = source(&stream_name)
        .create_if_missing(1)
        .subscribe(&connected)
        .await
        .expect("subscription opens on the existing stream");
    let unchanged = shards_of(&client, &stream_name).await;
    assert_eq!(
        unchanged.len(),
        3,
        "a stream that exists is used as it is, not reshaped to the descriptor's count",
    );
    drop(second);

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The partition key is the shard a record lands on, and the service picks that shard, not the
/// crate. One key means one shard - which is what buys per-key ordering and costs one shard's
/// throughput - and distinct keys spread over the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partition_key_pins_its_records_to_one_shard_and_distinct_keys_spread() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;
    let stream_name = unique("routing");

    let mut subscriber = from_horizon(&stream_name, 2)
        .subscribe(&connected)
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    for index in 0..PINNED {
        publish_keyed(
            &publisher,
            &stream_name,
            "tenant-acme",
            &format!("pinned-{index}"),
        )
        .await;
    }
    for index in 0..SPREAD {
        let name = format!("spread-{index}");
        publish_keyed(&publisher, &stream_name, &name, &name).await;
    }

    // Two shards deliver into one channel, so the arrival order across them is the readers' and
    // the assertion is about the grouping rather than the sequence.
    let mut by_payload: HashMap<String, String> = HashMap::new();
    let mut stream = pin!(subscriber.stream());
    for _ in 0..(PINNED + SPREAD) {
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        let shard = message
            .headers()
            .get_str(SHARD_HEADER)
            .expect("every delivery names its shard")
            .to_owned();
        let payload = String::from_utf8(message.payload().to_vec()).expect("the payload is text");
        by_payload.insert(payload, shard);
        message.ack().await.expect("ack succeeds");
    }

    let pinned: HashSet<&String> = (0..PINNED)
        .map(|index| {
            by_payload
                .get(&format!("pinned-{index}"))
                .expect("every pinned record arrives")
        })
        .collect();
    assert_eq!(
        pinned.len(),
        1,
        "records under one key must share one shard, got {pinned:?}",
    );
    let spread: HashSet<&String> = (0..SPREAD)
        .map(|index| {
            by_payload
                .get(&format!("spread-{index}"))
                .expect("every spread record arrives")
        })
        .collect();
    assert_eq!(
        spread.len(),
        2,
        "distinct keys must reach both shards of the stream, got {spread:?}",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A split is the moment the shard set changes under a running subscription. The crate's promise
/// is that the children start only once the parent is fully consumed, which is what keeps a key's
/// records in order across the split, and the parent is then marked finished for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_split_hands_the_stream_to_the_children_once_the_parent_is_consumed() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let client = observer(&endpoint).await;
    let leases = Arc::new(MemoryLeaseStore::new());
    let connected = connect_with(&endpoint, &leases).await;
    let stream_name = unique("split");

    let mut subscriber = from_horizon(&stream_name, 1)
        .subscribe(&connected)
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::new(&stream_name, b"before-the-split".as_slice()),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let before = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(before.payload(), b"before-the-split");
    let parent = before
        .headers()
        .get_str(SHARD_HEADER)
        .expect("every delivery names its shard")
        .to_owned();
    before.ack().await.expect("ack succeeds");

    split_in_half(&client, &stream_name, &parent).await;

    // Two keys, so the records reach both halves of the key space the children took over.
    for (index, key) in ["spread-0", "spread-1"].into_iter().enumerate() {
        publish_keyed(&publisher, &stream_name, key, &format!("after-{index}")).await;
    }

    let mut seen: HashSet<String> = HashSet::new();
    for _ in 0..2 {
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the children deliver once the parent is consumed")
            .expect("stream is open")
            .expect("delivery is ok");
        let shard = message
            .headers()
            .get_str(SHARD_HEADER)
            .expect("every delivery names its shard")
            .to_owned();
        assert_ne!(
            shard, parent,
            "a record published after the split arrives on a child, not on the closed parent",
        );
        seen.insert(String::from_utf8(message.payload().to_vec()).expect("the payload is text"));
        message.ack().await.expect("ack succeeds");
    }
    assert_eq!(
        seen,
        HashSet::from(["after-0".to_owned(), "after-1".to_owned()]),
        "both children must deliver what landed on them",
    );

    // The parent is finished for good: that checkpoint is the signal its children were allowed to
    // start, and it is what keeps a restarted service from reading the parent again.
    let state = leases
        .read(&parent)
        .await
        .expect("the store answers the parent's state");
    assert_eq!(
        state.checkpoint.as_deref(),
        Some(SHARD_END),
        "a fully consumed shard is checkpointed as finished",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}
