//! End-to-end checks against a local stack, gated behind `KINESIS_TEST_ENDPOINT`.
//!
//! What lives here is what only a server can answer: the wire the crate writes and reads back,
//! and the shard-lease and checkpoint semantics behind acknowledgement. Handler behaviour -
//! delivery contexts, repositioning from a handler, batch bodies - is a service-level concern and
//! is covered on the framework's harness in `harness_kinesis.rs`; the `Seekable` contract itself
//! is covered by the framework's own suite in `conformance_kinesis.rs`, in process and against
//! this stack.
//!
//! Start one with `just brokers-up`, then:
//! `KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use tokio::sync::Notify;

use ruststream::runtime::PublishExt;
use ruststream::{
    Broker, ConnectedBroker, HeaderMap, IncomingMessage, Outgoing, OutgoingMessage, Publisher,
    Serialized, StartAt, Subscriber, SubscriptionSource,
};
use ruststream_kinesis::{
    ConnectedKinesisBroker, KinesisBroker, KinesisPosition, KinesisPublishSteps, KinesisStream,
    LeaseError, LeaseState, LeaseStore, MemoryLeaseStore, PARTITION_KEY_HEADER, SEQUENCE_HEADER,
};

mod live;

const RECV_TIMEOUT: Duration = Duration::from_secs(30);

/// The stack's endpoint, or `None` to skip the live checks below. Under `RUSTSTREAM_REQUIRE_LIVE`
/// a missing endpoint is a failure instead, so a job that started a stack cannot report `ok`
/// without having reached it.
fn test_endpoint() -> Option<String> {
    live::url("KINESIS_TEST_ENDPOINT")
}

/// A lease store that announces a release.
///
/// Handing a shard back is the moment a dropped subscription's reader has stopped, and it is the
/// only such moment a test can observe: the reader is a detached task, so waiting for it means
/// waiting on the store it coordinates through. Leasing is a pluggable trait precisely so a
/// deployment can supply its own, and a test is a deployment.
#[derive(Debug, Default)]
struct ReleaseWatch {
    inner: MemoryLeaseStore,
    released: Notify,
}

impl ReleaseWatch {
    /// Resolves once a reader has handed a shard back. `notify_one` stores a permit, so a
    /// release that happened before this call is not missed.
    async fn released(&self) {
        self.released.notified().await;
    }
}

impl LeaseStore for ReleaseWatch {
    fn acquire<'a>(
        &'a self,
        shard: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        self.inner.acquire(shard, owner, ttl)
    }

    fn renew<'a>(
        &'a self,
        shard: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        self.inner.renew(shard, owner, ttl)
    }

    fn checkpoint<'a>(
        &'a self,
        shard: &'a str,
        owner: &'a str,
        sequence: &'a str,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        self.inner.checkpoint(shard, owner, sequence)
    }

    fn read<'a>(&'a self, shard: &'a str) -> BoxFuture<'a, Result<LeaseState, LeaseError>> {
        self.inner.read(shard)
    }

    fn release<'a>(
        &'a self,
        shard: &'a str,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<(), LeaseError>> {
        Box::pin(async move {
            let outcome = self.inner.release(shard, owner).await;
            self.released.notify_one();
            outcome
        })
    }
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

/// Connects with a lease store the test can wait on, for the checks that hand a shard from one
/// subscription to the next.
async fn connect_watching(endpoint: &str, leases: &Arc<ReleaseWatch>) -> ConnectedKinesisBroker {
    broker(endpoint)
        .lease_store(Arc::clone(leases) as Arc<dyn LeaseStore>)
        .connect()
        .await
        .expect("broker connects")
}

/// Per-test unique stream, so runs do not observe each other's leftovers.
fn unique(name: &str) -> String {
    format!("it-{name}-{}", std::process::id())
}

/// The plain descriptor: every shard resumes from its checkpoint, and a shard without one
/// opens at the tip.
fn source(stream: &str) -> KinesisStream {
    KinesisStream::new(stream)
        .create_if_missing(1)
        .poll_interval(Duration::from_millis(200))
}

/// The same descriptor opened at the trim horizon, the way `start_at(..)` opens it: a forced
/// position, so it also beats a stored checkpoint.
fn from_horizon(stream: &str) -> StartAt<KinesisStream, KinesisPosition> {
    StartAt::new(source(stream), KinesisPosition::horizon())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roundtrip_preserves_payload_headers_and_partition_key() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let stream_name = unique("roundtrip");
    let mut subscriber = from_horizon(&stream_name)
        .subscribe(&connected)
        .await
        .expect("subscription opens");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    headers.insert(PARTITION_KEY_HEADER, "user-42");
    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::new(&stream_name, b"{\"id\":1}".as_slice()).with_headers(headers),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(message.payload(), b"{\"id\":1}");
    assert_eq!(
        message.headers().get_str("content-type"),
        Some("application/json")
    );
    assert_eq!(message.headers().get_str("x-tenant"), Some("acme"));
    assert_eq!(message.partition_key(), Some(b"user-42".as_slice()));
    assert!(message.headers().get_str(SEQUENCE_HEADER).is_some());
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Bytes the test already holds encoded: a serialized type reaches the stream as it is, so this
/// check stays about the partition key rather than about a codec.
#[derive(Outgoing, Serialized)]
struct Seed(Vec<u8>);

/// The partition key a call names has to land in the record's own field, which is the only place
/// Kinesis keeps one - the header a delivery reports is rebuilt from it, so only a server can
/// answer whether the step reached the record or merely the header map.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_step_puts_the_partition_key_on_the_record() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let stream_name = unique("keyed");
    let mut subscriber = from_horizon(&stream_name)
        .subscribe(&connected)
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .message(&Seed(b"{\"id\":1}".to_vec()))
        .to(stream_name.as_str())
        .partition_key("tenant-acme")
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(message.partition_key(), Some(b"tenant-acme".as_slice()));
    assert_eq!(message.payload(), b"{\"id\":1}");
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoints_resume_where_acknowledgement_stopped() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let leases = Arc::new(ReleaseWatch::default());
    let connected = connect_watching(&endpoint, &leases).await;

    let stream_name = unique("resume");
    let publisher = connected.publisher();

    // First pass: consume and acknowledge two records.
    {
        let mut subscriber = from_horizon(&stream_name)
            .subscribe(&connected)
            .await
            .expect("subscription opens");
        for payload in [b"one".as_slice(), b"two".as_slice()] {
            publisher
                .publish(OutgoingMessage::new(&stream_name, payload), None)
                .await
                .expect("publish succeeds");
        }
        let mut stream = pin!(subscriber.stream());
        for _ in 0..2 {
            let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .expect("delivery arrives")
                .expect("stream is open")
                .expect("delivery is ok");
            message.ack().await.expect("ack succeeds");
        }
    }
    // The dropped subscription's reader hands the shard back on its way out; that release is the
    // signal, not a guessed teardown delay.
    tokio::time::timeout(RECV_TIMEOUT, leases.released())
        .await
        .expect("the first reader releases its shard");

    // Second pass over the same broker (same lease store), on the plain descriptor: with no
    // position forced, the shard resumes from its checkpoint and the acknowledged records
    // stay consumed.
    publisher
        .publish(
            OutgoingMessage::new(&stream_name, b"three".as_slice()),
            None,
        )
        .await
        .expect("publish succeeds");
    let mut subscriber = source(&stream_name)
        .subscribe(&connected)
        .await
        .expect("subscription reopens");
    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"three");
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unacknowledged_record_replays_on_the_next_lease() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let leases = Arc::new(ReleaseWatch::default());
    let connected = connect_watching(&endpoint, &leases).await;

    let stream_name = unique("replay");
    let publisher = connected.publisher();

    // The first record is acknowledged, so it checkpoints; the second is not, which wedges
    // the watermark there even though the third is handled.
    {
        let mut subscriber = from_horizon(&stream_name)
            .subscribe(&connected)
            .await
            .expect("subscription opens");
        for payload in [
            b"first".as_slice(),
            b"sticky".as_slice(),
            b"after".as_slice(),
        ] {
            publisher
                .publish(OutgoingMessage::new(&stream_name, payload), None)
                .await
                .expect("publish succeeds");
        }
        let mut stream = pin!(subscriber.stream());
        for expected in [
            b"first".as_slice(),
            b"sticky".as_slice(),
            b"after".as_slice(),
        ] {
            let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .expect("delivery arrives")
                .expect("stream is open")
                .expect("delivery is ok");
            assert_eq!(message.payload(), expected);
            if expected == b"sticky" {
                // nack(requeue = true): leave it unhandled - the watermark must not advance.
                message.nack(true).await.expect("nack succeeds");
            } else {
                message.ack().await.expect("ack succeeds");
            }
        }
    }
    tokio::time::timeout(RECV_TIMEOUT, leases.released())
        .await
        .expect("the first reader releases its shard");

    // The plain descriptor resumes from the checkpoint, which never moved past the
    // unacknowledged record: it and everything after it are delivered again.
    let mut subscriber = source(&stream_name)
        .subscribe(&connected)
        .await
        .expect("subscription reopens");
    let mut stream = pin!(subscriber.stream());
    let replayed = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("replay arrives")
        .expect("stream is open")
        .expect("replay is ok");
    assert_eq!(replayed.payload(), b"sticky");
    replayed.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}
