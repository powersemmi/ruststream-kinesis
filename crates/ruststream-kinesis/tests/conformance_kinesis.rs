//! Conformance: the routing suite against the in-process transport, and the lifecycle check
//! against a local stack (gated behind `KINESIS_TEST_ENDPOINT`).
//!
//! Start one with `just brokers-up`, then:
//! `KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features`.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::StartAt;
use ruststream::conformance::{capabilities, harness};
use ruststream_kinesis::testing::KinesisTestBroker;
use ruststream_kinesis::{KinesisBroker, KinesisPosition, KinesisStream};

fn test_endpoint() -> Option<String> {
    match std::env::var("KINESIS_TEST_ENDPOINT") {
        Ok(endpoint) if !endpoint.is_empty() => Some(endpoint),
        _ => {
            eprintln!("KINESIS_TEST_ENDPOINT is not set; skipping the live conformance check");
            None
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_test_broker_passes_conformance_suite() {
    harness::run_suite(KinesisTestBroker::new).await;
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
