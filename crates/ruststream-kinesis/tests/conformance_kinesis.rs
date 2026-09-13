//! Conformance: the framework's contract suites, each run twice - against the in-process
//! transport, and against a local stack (gated behind `KINESIS_TEST_ENDPOINT`).
//!
//! Both legs matter. The in-process leg is what holds the stand-in to the same contract the
//! service answers to, so an emulation cannot quietly drift into something more convenient than
//! the product; the live leg is what proves the contract itself is the product's, and not one the
//! stand-in was written to satisfy.
//!
//! `just test-brokers` runs them against a stack it starts and removes. Driving them by hand
//! (`just brokers-up`, then `KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test
//! --all-features`) works once per stack: the framework's suites publish under fixed subjects,
//! and a Kinesis stream retains what an earlier run wrote, so a second run against the same
//! container replays the first one's records into it. The recipe removes the volume between
//! runs for that reason.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::StartAt;
use ruststream::conformance::{capabilities, harness};
use ruststream_kinesis::testing::KinesisTestBroker;
use ruststream_kinesis::{KinesisBroker, KinesisPosition, KinesisStream};

mod live;

/// The stack's endpoint, or `None` to skip the live checks below. Under `RUSTSTREAM_REQUIRE_LIVE`
/// a missing endpoint is a failure instead, so a job that started a stack cannot report `ok`
/// without having reached it.
fn test_endpoint() -> Option<String> {
    live::url("KINESIS_TEST_ENDPOINT")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_test_broker_passes_conformance_suite() {
    harness::run_suite(KinesisTestBroker::new).await;
}

/// The lifecycle ladder against the stand-in: synchronous construction, the consuming `connect`,
/// a subscription opened through the crate's own descriptor, a publish it receives and acks, the
/// consuming `shutdown`, and - the part only a runtime check can hold - a publisher created
/// before the shutdown erroring afterwards instead of succeeding against a closed transport.
///
/// The stand-in follows the same ladder as the service, so it owes the same answers; the live leg
/// below runs this very suite against the product.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_test_broker_passes_lifecycle() {
    harness::lifecycle(
        KinesisTestBroker::new,
        |name| KinesisStream::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// A Kinesis stream addresses its own retry copies, so the descriptor promises that a publish to
/// the address it reports arrives at the subscription that reported it. That promise is the whole
/// deferred retry: an address reaching nothing would lose every delayed record silently.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_test_broker_reports_a_reachable_redelivery_address() {
    harness::redelivery_address(
        KinesisTestBroker::new,
        |name| KinesisStream::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The stand-in claims `Seekable` and `Positioned` over its retained log, so it owes the same
/// contract the service does: a captured position redelivers exactly its record and the ordered
/// suffix after it, and a forward seek skips what was queued. Running the framework's own suite
/// against it is what keeps that emulation from decaying into a handle that accepts every seek
/// and moves nothing.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_test_broker_passes_seeking_suite() {
    capabilities::seeking(
        KinesisTestBroker::new,
        |name| KinesisStream::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The batch size is the one subscription parameter the framework carries down, so the size a
/// mount site names has to be the size a batch comes back at. The suite opens its subscription
/// smaller than the run it publishes, which is what catches a broker that ignores it.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_test_broker_passes_batch_suite() {
    capabilities::batches(
        KinesisTestBroker::new,
        |name| KinesisStream::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_passes_lifecycle() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    harness::lifecycle(
        || {
            KinesisBroker::new()
                .endpoint(endpoint.clone())
                .test_credentials()
                .region("us-east-1")
        },
        |name| {
            StartAt::new(
                KinesisStream::new(name).create_if_missing(1),
                KinesisPosition::horizon(),
            )
        },
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_passes_seeking_suite() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    capabilities::seeking(
        || {
            KinesisBroker::new()
                .endpoint(endpoint.clone())
                .test_credentials()
                .region("us-east-1")
        },
        |name| {
            StartAt::new(
                KinesisStream::new(name)
                    .create_if_missing(1)
                    .poll_interval(Duration::from_millis(200)),
                KinesisPosition::horizon(),
            )
        },
        |connected| connected.publisher(),
    )
    .await;
}

/// The same promise against the service, where the address is a real stream name and the publish
/// is a real `PutRecord`: only a server shows that a copy published under the reported name is
/// picked up by the shard reader the descriptor opened.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_reports_a_reachable_redelivery_address() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    harness::redelivery_address(
        || {
            KinesisBroker::new()
                .endpoint(endpoint.clone())
                .test_credentials()
                .region("us-east-1")
        },
        |name| {
            StartAt::new(
                KinesisStream::new(name)
                    .create_if_missing(1)
                    .poll_interval(Duration::from_millis(200)),
                KinesisPosition::horizon(),
            )
        },
        |connected| connected.publisher(),
    )
    .await;
}

/// The same batch contract against the service, where the size is a `GetRecords` limit rather
/// than a client-side cap: only a server can show that the reader asks for it and that a read
/// answering with fewer records still yields a batch.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_passes_batch_suite() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    capabilities::batches(
        || {
            KinesisBroker::new()
                .endpoint(endpoint.clone())
                .test_credentials()
                .region("us-east-1")
        },
        |name| {
            StartAt::new(
                KinesisStream::new(name)
                    .create_if_missing(1)
                    .poll_interval(Duration::from_millis(200)),
                KinesisPosition::horizon(),
            )
        },
        |connected| connected.publisher(),
    )
    .await;
}
