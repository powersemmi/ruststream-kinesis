//! The broker's in-process mode, driven directly: the transport is the subject here, so these
//! cases open subscriptions and publish on the connected form `connect_in_process` produces, the
//! one the `TestApp` harness connects.
//!
//! Each case is a place where the in-process transport answers what the service answers: the
//! record it refuses, the record it delivers, and what a record left unhandled does. What only a
//! server does - shards, leases, checkpoints - is covered against a local stack by
//! `integration_kinesis.rs` and `shards_kinesis.rs`.

#![cfg(feature = "testing")]

use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::runtime::PublishExt;
use ruststream::testing::{InProcess, TestableBroker};
use ruststream::{
    Bytes, BytesMut, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher,
    Seekable, Seeker, Subscriber,
};
use ruststream_kinesis::{
    ConnectedKinesisBroker, KinesisBroker, KinesisError, KinesisMessage, KinesisPosition,
    KinesisPublishSteps, KinesisStream, KinesisSubscriber, PARTITION_KEY_HEADER, SEQUENCE_HEADER,
};

/// How long a delivery that is due may take to arrive.
const WAIT: Duration = Duration::from_secs(1);

/// How long a delivery that is not due is waited for before the case calls it absent.
const ABSENT: Duration = Duration::from_millis(100);

/// The transition the harness connects through: the production broker, connected in process.
async fn connected() -> ConnectedKinesisBroker {
    KinesisBroker::new()
        .connect_in_process()
        .await
        .expect("the broker connects in process")
}

async fn subscribe(broker: &ConnectedKinesisBroker, stream: &str) -> KinesisSubscriber {
    broker
        .subscribe_stream(KinesisStream::new(stream))
        .await
        .expect("the subscription opens")
}

async fn publish(broker: &ConnectedKinesisBroker, stream: &str, payload: &'static [u8]) {
    broker
        .publisher()
        .publish(
            OutgoingMessage::produced(stream, BytesMut::from(payload)),
            None,
        )
        .await
        .expect("the publish succeeds");
}

async fn next<S>(deliveries: &mut S) -> Result<KinesisMessage, KinesisError>
where
    S: Stream<Item = Result<KinesisMessage, KinesisError>> + Unpin,
{
    tokio::time::timeout(WAIT, deliveries.next())
        .await
        .expect("a delivery arrives")
        .expect("the subscription is open")
}

async fn nothing_more<S>(deliveries: &mut S)
where
    S: Stream<Item = Result<KinesisMessage, KinesisError>> + Unpin,
{
    assert!(
        tokio::time::timeout(ABSENT, deliveries.next())
            .await
            .is_err(),
        "no delivery is due"
    );
}

// A stream name is letters, digits, `_`, `.` and `-`, up to 128 of them. The service refuses any
// other, when a subscription asks for the stream and when a record is written to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_name_the_service_refuses_is_refused() {
    let broker = connected().await;

    let refused = broker
        .subscribe_stream(KinesisStream::new("orders/eu"))
        .await
        .expect_err("a slash is not in a stream name");
    assert!(
        matches!(refused, KinesisError::Stream { .. }),
        "got {refused}"
    );

    let refused = broker
        .publisher()
        .publish(
            OutgoingMessage::produced(&"s".repeat(129), BytesMut::from(&b"x"[..])),
            None,
        )
        .await
        .expect_err("a stream name is at most 128 characters");
    assert!(
        matches!(refused, KinesisError::Publish { .. }),
        "got {refused}"
    );
}

// A partition key is 1 to 256 characters, and a record carries at most 1 MiB of data and key
// together. The service refuses a record outside either limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_the_service_refuses_is_refused() {
    let broker = connected().await;
    let publisher = broker.publisher();

    let refused = publisher
        .message(&Raw(b"x".to_vec()))
        .to("orders")
        .partition_key("k".repeat(257))
        .publish()
        .await;
    assert!(refused.is_err(), "a 257-character key is refused");

    let mut empty_key = HeaderMap::new();
    empty_key.insert(PARTITION_KEY_HEADER, "");
    let refused = publisher
        .publish(
            OutgoingMessage::produced("orders", BytesMut::from(&b"x"[..])).with_headers(empty_key),
            None,
        )
        .await
        .expect_err("an empty key is refused");
    assert!(
        matches!(refused, KinesisError::Publish { .. }),
        "got {refused}"
    );

    let refused = publisher
        .publish(
            OutgoingMessage::produced("orders", BytesMut::zeroed(1024 * 1024)),
            None,
        )
        .await
        .expect_err("a record over 1 MiB is refused");
    assert!(
        matches!(refused, KinesisError::Publish { .. }),
        "got {refused}"
    );
    assert!(broker.published("orders").is_empty());
}

/// Bytes the case already holds encoded, published as they are.
#[derive(ruststream::Outgoing, ruststream::Serialized)]
struct Raw(Vec<u8>);

// A sequence number is a decimal wider than any integer type, as the service's are, so a handler
// that reads one as a number fails here where it would fail against the service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sequence_number_is_as_wide_as_the_services() {
    let broker = connected().await;
    let mut subscriber = subscribe(&broker, "orders").await;
    publish(&broker, "orders", b"one").await;

    let mut deliveries = Box::pin(subscriber.stream());
    let delivery = next(&mut deliveries)
        .await
        .expect("the record is delivered");
    let sequence = delivery
        .headers()
        .get_str(SEQUENCE_HEADER)
        .expect("the delivery reports its sequence number")
        .to_owned();

    assert_eq!(sequence.len(), 56, "got {sequence}");
    assert!(sequence.parse::<u128>().is_err(), "got {sequence}");
}

// A record carries a data blob and a partition key, so the headers travel in the envelope the
// publisher writes, and a delivery reads them back out of it: a value that is not text arrives as
// the text the envelope made of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_arrives_as_the_envelope_carries_it() {
    let broker = connected().await;
    let mut subscriber = subscribe(&broker, "orders").await;
    let mut headers = HeaderMap::new();
    headers.insert("x-raw", Bytes::from_static(&[0xff, 0xfe]));
    broker
        .publisher()
        .publish(
            OutgoingMessage::produced("orders", BytesMut::from(&b"one"[..])).with_headers(headers),
            None,
        )
        .await
        .expect("the publish succeeds");

    let mut deliveries = Box::pin(subscriber.stream());
    let delivery = next(&mut deliveries)
        .await
        .expect("the record is delivered");

    assert_eq!(delivery.payload(), b"one");
    assert_eq!(
        delivery.headers().get_str("x-raw"),
        Some("\u{fffd}\u{fffd}")
    );
}

// A record the producer aggregated is refused, as the service's reader refuses it, and the stream
// reads on past it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aggregated_record_is_refused_and_the_stream_reads_on() {
    let broker = connected().await;
    let mut subscriber = subscribe(&broker, "orders").await;
    publish(&broker, "orders", &[0xf3, 0x89, 0x9a, 0xc2, 0x01]).await;
    publish(&broker, "orders", b"plain").await;

    let mut deliveries = Box::pin(subscriber.stream());
    let refused = next(&mut deliveries)
        .await
        .expect_err("an aggregated record is not delivered");
    assert!(
        matches!(refused, KinesisError::AggregatedRecord { .. }),
        "got {refused}"
    );
    let delivery = next(&mut deliveries)
        .await
        .expect("the next record is delivered");
    assert_eq!(delivery.payload(), b"plain");
}

// A record left unhandled stops the shard's watermark, and the shard is read again from it: the
// record comes back, and so does every record after it, handled or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_left_unhandled_comes_back_with_every_record_after_it() {
    let broker = connected().await;
    let mut subscriber = subscribe(&broker, "orders").await;
    publish(&broker, "orders", b"one").await;
    publish(&broker, "orders", b"two").await;

    let mut deliveries = Box::pin(subscriber.stream());
    let one = next(&mut deliveries).await.expect("one is delivered");
    let two = next(&mut deliveries).await.expect("two is delivered");
    one.nack(true)
        .await
        .expect("leaving a record unhandled succeeds");
    two.ack().await.expect("the ack succeeds");

    let again: Vec<Vec<u8>> = [
        next(&mut deliveries).await.expect("one comes back"),
        next(&mut deliveries).await.expect("two comes back"),
    ]
    .into_iter()
    .map(|delivery| delivery.payload().to_vec())
    .collect();
    assert_eq!(again, [b"one".to_vec(), b"two".to_vec()]);
    nothing_more(&mut deliveries).await;
}

// A reposition abandons the read it interrupted, so a record delivered before it and left
// unhandled after it moves nothing: the service's reader settles such a record against a
// watermark the reposition already reset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_left_unhandled_after_a_seek_moves_nothing() {
    let broker = connected().await;
    let mut subscriber = subscribe(&broker, "orders").await;
    let seeker = subscriber.seeker();
    publish(&broker, "orders", b"one").await;

    let mut deliveries = Box::pin(subscriber.stream());
    let one = next(&mut deliveries).await.expect("one is delivered");
    seeker
        .seek(KinesisPosition::latest())
        .await
        .expect("the seek succeeds");
    one.nack(true)
        .await
        .expect("leaving a record unhandled succeeds");

    nothing_more(&mut deliveries).await;
}

// A publisher handed out before the broker connected publishes over the connection the broker
// gets, in process as against the service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publisher_handed_out_before_connect_publishes_in_process() {
    let broker = KinesisBroker::new();
    let early = broker.publisher();
    let connected = broker
        .connect_in_process()
        .await
        .expect("the broker connects in process");
    let mut subscriber = subscribe(&connected, "orders").await;

    early
        .publish(
            OutgoingMessage::produced("orders", BytesMut::from(&b"early"[..])),
            None,
        )
        .await
        .expect("the publish succeeds");

    let mut deliveries = Box::pin(subscriber.stream());
    let delivery = next(&mut deliveries)
        .await
        .expect("the record is delivered");
    assert_eq!(delivery.payload(), b"early");
}

// A seek handle outlives the connection it was minted on, and after `shutdown` it reports the
// closed connection rather than repositioning a subscription nothing reads any more.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seek_handle_reports_the_closed_connection_after_shutdown() {
    let broker = connected().await;
    let subscriber = subscribe(&broker, "orders").await;
    let seeker = subscriber.seeker();

    broker.shutdown().await.expect("the broker shuts down");

    let refused = seeker
        .seek(KinesisPosition::horizon())
        .await
        .expect_err("the connection is closed");
    assert!(
        matches!(refused, KinesisError::NotConnected),
        "got {refused}"
    );
}

// Every subscription on a stream reads every record written to it, and a subscription on another
// stream reads none: the rule the harness waits on when it runs the same test live.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_reaches_every_subscription_on_its_stream() {
    let broker = connected().await;
    let mut first = subscribe(&broker, "orders").await;
    let mut second = subscribe(&broker, "orders").await;
    let mut other = subscribe(&broker, "payments").await;
    publish(&broker, "orders", b"one").await;

    for subscriber in [&mut first, &mut second] {
        let mut deliveries = Box::pin(subscriber.stream());
        let delivery = next(&mut deliveries)
            .await
            .expect("the record is delivered");
        assert_eq!(delivery.payload(), b"one");
    }
    nothing_more(&mut Box::pin(other.stream())).await;

    assert_eq!(
        broker.routes("orders", &["orders", "payments", "orders"]),
        [0, 2]
    );
    assert!(broker.routes("orders", &["orders.dead"]).is_empty());
}
