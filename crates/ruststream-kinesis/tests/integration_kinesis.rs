//! End-to-end checks against a local stack, gated behind `KINESIS_TEST_ENDPOINT`.
//!
//! Start one with `just brokers-up`, then:
//! `KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, Headers, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_kinesis::{
    ConnectedKinesisBroker, KinesisBroker, KinesisStream, PARTITION_KEY_HEADER, SEQUENCE_HEADER,
    StartPosition,
};

const RECV_TIMEOUT: Duration = Duration::from_secs(30);

fn test_endpoint() -> Option<String> {
    match std::env::var("KINESIS_TEST_ENDPOINT") {
        Ok(endpoint) if !endpoint.is_empty() => Some(endpoint),
        _ => {
            eprintln!("KINESIS_TEST_ENDPOINT is not set; skipping the live integration test");
            None
        }
    }
}

async fn connect(endpoint: &str) -> ConnectedKinesisBroker {
    KinesisBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
        .connect()
        .await
        .expect("broker connects")
}

/// Per-test unique stream, so runs do not observe each other's leftovers.
fn unique(name: &str) -> String {
    format!("it-{name}-{}", std::process::id())
}

fn source(stream: &str) -> KinesisStream {
    KinesisStream::new(stream)
        .create_if_missing(1)
        .start(StartPosition::Horizon)
        .poll_interval(Duration::from_millis(200))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roundtrip_preserves_payload_headers_and_partition_key() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let stream_name = unique("roundtrip");
    let mut subscriber = connected
        .subscribe_stream(source(&stream_name))
        .await
        .expect("subscription opens");

    let mut headers = Headers::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    headers.insert(PARTITION_KEY_HEADER, "user-42");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&stream_name, b"{\"id\":1}".as_slice()).with_headers(headers))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoints_resume_where_acknowledgement_stopped() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let stream_name = unique("resume");
    let publisher = connected.publisher();

    // First pass: consume and acknowledge two records.
    {
        let mut subscriber = connected
            .subscribe_stream(source(&stream_name))
            .await
            .expect("subscription opens");
        for payload in [b"one".as_slice(), b"two".as_slice()] {
            publisher
                .publish(OutgoingMessage::new(&stream_name, payload))
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
    // Give the dropped subscription's tasks a moment to settle their teardown.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Second pass over the same broker (same lease store): the checkpoint skips the
    // acknowledged records even though the start position is the horizon.
    publisher
        .publish(OutgoingMessage::new(&stream_name, b"three".as_slice()))
        .await
        .expect("publish succeeds");
    let mut subscriber = connected
        .subscribe_stream(source(&stream_name))
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
    let connected = connect(&endpoint).await;

    let stream_name = unique("replay");
    let publisher = connected.publisher();

    {
        let mut subscriber = connected
            .subscribe_stream(source(&stream_name))
            .await
            .expect("subscription opens");
        publisher
            .publish(OutgoingMessage::new(&stream_name, b"sticky".as_slice()))
            .await
            .expect("publish succeeds");
        let mut stream = pin!(subscriber.stream());
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        // nack(requeue = true): leave it unhandled - the watermark must not advance.
        message.nack(true).await.expect("nack succeeds");
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut subscriber = connected
        .subscribe_stream(source(&stream_name))
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
