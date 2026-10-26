//! The header envelope seen from a service: a payload reaches the handler exactly as it was
//! published, whatever its first bytes are. One body runs in process and against a local stack.
#![cfg(feature = "testing")]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ruststream::testing::TestApp;
use ruststream_kinesis::prelude::*;

use crate::live;

/// The stream the test publishes to.
const FRAMES: &str = "frames.wire";

/// The stream the service forwards every frame to.
const ARCHIVE: &str = "frames.archive";

/// Payloads that begin like the envelope this crate writes and like the one earlier releases
/// wrote, each followed by a header block that parses. Read as envelopes they would lose their
/// first bytes, and the second would also invent a header.
const LOOKALIKES: [&[u8]; 2] = [b"RSK2\0\0\0\0frame", b"RSK1\0\0\0\x05a: b\nframe"];

/// A frame is the payload as the record carries it.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// The frame the service forwards, written as it was read.
#[derive(Outgoing, Serialized)]
struct Archived(Vec<u8>);

#[subscriber(
    KinesisStream::new(FRAMES)
        .create_if_missing(1)
        .poll_interval(Duration::from_millis(200)),
    reply(ARCHIVE)
)]
async fn forward(frame: &Frame<'_>) -> Archived {
    Archived(frame.0.to_vec())
}

#[subscriber(
    KinesisStream::new(ARCHIVE)
        .create_if_missing(1)
        .poll_interval(Duration::from_millis(200))
)]
async fn archive(_frame: &Frame<'_>) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// The app `main` runs, on the broker it is handed. Both subscriptions start at the moment the
/// app is built, so records a previous run left on the stack are not read.
fn app(broker: KinesisBroker) -> RustStream {
    let opened = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past the epoch");
    let opened = u64::try_from(opened.as_millis()).expect("the instant fits in milliseconds");
    RustStream::new(AppInfo::new("frames", "0.1.0")).with_broker(broker, |b| {
        b.include(forward.start_at(KinesisPosition::timestamp(opened)));
        b.include(archive.start_at(KinesisPosition::timestamp(opened)));
    })
}

/// The body both modes run: a test publish without headers, then the service's own reply
/// without headers, each read back byte for byte.
async fn a_payload_that_looks_like_an_envelope_arrives_unchanged(tb: TestApp<()>) {
    let broker = tb.broker::<KinesisBroker>();
    for payload in LOOKALIKES {
        broker
            .message(&Archived(payload.to_vec()))
            .to(FRAMES)
            .publish()
            .await
            .expect("publish drives the frame and its reply to their settlement");
    }

    let expected: Vec<&[u8]> = LOOKALIKES.to_vec();
    for stream in [FRAMES, ARCHIVE] {
        let received = broker.subscriber(stream).assert_called(2).received_raw();
        assert_eq!(
            received.iter().map(AsRef::as_ref).collect::<Vec<&[u8]>>(),
            expected,
            "on {stream}"
        );
    }

    tb.shutdown().await.expect("the harness shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_payload_that_looks_like_an_envelope_arrives_unchanged_in_process() {
    let tb = TestApp::start(app(KinesisBroker::new()))
        .await
        .expect("the harness starts");
    a_payload_that_looks_like_an_envelope_arrives_unchanged(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_payload_that_looks_like_an_envelope_arrives_unchanged_live() {
    let Some(endpoint) = live::url("KINESIS_TEST_ENDPOINT") else {
        return;
    };
    let broker = KinesisBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1");
    let tb = TestApp::start_live(app(broker))
        .await
        .expect("the harness starts against the stand");
    a_payload_that_looks_like_an_envelope_arrives_unchanged(tb).await;
}
