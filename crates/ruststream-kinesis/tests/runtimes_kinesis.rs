//! Requests from a runtime other than the one the broker connected on, against a local stack,
//! gated behind `KINESIS_TEST_ENDPOINT`.
//!
//! A handler on a dedicated thread runs on a current-thread runtime of its own. A connection the
//! SDK opens there is driven by that runtime alone, so it must never serve a request from the
//! broker's runtime: while the thread computes, such a request would wait for it.
//!
//! Start a stack with `just brokers-up`, then:
//! `KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

use std::thread;
use std::time::{Duration, Instant};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_kinesis::client::Waiters;
use ruststream::{Broker, ConnectedBroker, OutgoingMessage, Publisher};
use ruststream_kinesis::KinesisBroker;
use tokio::runtime;
use tokio::sync::oneshot;

mod live;

/// How long the dedicated thread computes after its publish: far past [`PUBLISH_TIMEOUT`], so a
/// request that waits for the thread cannot finish in time.
const BUSY: Duration = Duration::from_secs(3);

/// What a publish from the broker's runtime may take against a local stack.
const PUBLISH_TIMEOUT: Duration = Duration::from_millis(500);

fn test_endpoint() -> Option<String> {
    live::url("KINESIS_TEST_ENDPOINT")
}

/// Creates the stream through a client of the test's own, so the broker's connections are the
/// ones the publishes below open and nothing else.
async fn create_stream(endpoint: &str, stream: &str) {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .test_credentials()
        .load()
        .await;
    let client = aws_sdk_kinesis::Client::new(&config);
    // A stream left by an earlier run is fine: only its existence matters here.
    let _ = client
        .create_stream()
        .stream_name(stream)
        .shard_count(1)
        .send()
        .await;
    client
        .wait_until_stream_exists()
        .stream_name(stream)
        .wait(Duration::from_mins(1))
        .await
        .expect("the stream becomes active");
}

/// A publish from the broker's runtime completes while a dedicated thread that published before
/// is computing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_does_not_wait_for_a_busy_thread_that_published_before() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let stream = format!("rt-busy-thread-{}", std::process::id());
    create_stream(&endpoint, &stream).await;
    let connected = KinesisBroker::new()
        .endpoint(&endpoint)
        .test_credentials()
        .region("us-east-1")
        .connect()
        .await
        .expect("broker connects");

    let (published, on_published) = oneshot::channel();
    let from_thread = connected.publisher();
    let thread_stream = stream.clone();
    let busy = thread::spawn(move || {
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the thread's runtime builds");
        runtime
            .block_on(from_thread.publish(
                OutgoingMessage::new(&thread_stream, b"from the thread".as_slice()),
                None,
            ))
            .expect("the thread's publish succeeds");
        published.send(()).expect("the test waits for the thread");
        // The thread computes with its runtime still alive: nothing on that runtime is polled
        // until the loop ends.
        let until = Instant::now() + BUSY;
        while Instant::now() < until {
            std::hint::spin_loop();
        }
        drop(runtime);
    });
    on_published.await.expect("the thread published");

    let started = Instant::now();
    let outcome = tokio::time::timeout(
        PUBLISH_TIMEOUT,
        connected.publisher().publish(
            OutgoingMessage::new(&stream, b"from the broker's runtime".as_slice()),
            None,
        ),
    )
    .await;
    let took = started.elapsed();
    tokio::task::spawn_blocking(move || busy.join())
        .await
        .expect("the join task runs")
        .expect("the thread finishes");

    outcome
        .unwrap_or_else(|_| {
            panic!("the publish waited for the busy thread: no answer within {PUBLISH_TIMEOUT:?}")
        })
        .expect("the publish succeeds");
    eprintln!("publish from the broker's runtime took {took:?}");
    connected.shutdown().await.expect("shutdown succeeds");
}
