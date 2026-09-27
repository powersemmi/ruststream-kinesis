//! Conformance: the framework's contract suites, each run over the same production broker -
//! connected in process, and against a local stack (gated behind `KINESIS_TEST_ENDPOINT`).
//!
//! Both legs matter. The in-process leg, through `harness::InProcessBroker`, is what holds the
//! in-process mode to the same contract the service answers to, so the transport cannot quietly
//! drift into something more convenient than the product; the live leg is what proves the contract
//! itself is the product's, and not one the in-process transport was written to satisfy.
//!
//! `just test-brokers` runs them against a stack it starts and removes. Every suite publishes
//! under a subject of its own, so driving them by hand (`just brokers-up`, then
//! `KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features`) is repeatable
//! against one stack.

#![cfg(feature = "testing")]
// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#![allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]

use std::sync::Arc;
use std::time::Duration;

use ruststream::conformance::harness::{self, InProcessBroker};
use ruststream::conformance::helpers::unique_subject;
use ruststream::conformance::message_shape::{self, OptionCases};
use ruststream::conformance::{capabilities, lifecycle, retry, settlement};
use ruststream::testing::Backlog;
use ruststream::{IncomingMessage, Name, StartAt};
use ruststream_kinesis::{
    ConnectedKinesisBroker, KinesisBroker, KinesisMessage, KinesisPosition, KinesisPublish,
    KinesisPublishOptions, KinesisStream, LeaseStore, MemoryLeaseStore,
};

mod live;

/// The partition key the options check configures on its policy, one no publish picks by itself.
const POLICY_KEY: &str = "conformance-policy-key";

/// The stack's endpoint, or `None` to skip the live checks below. Under `RUSTSTREAM_REQUIRE_LIVE`
/// a missing endpoint is a failure instead, so a job that started a stack cannot report `ok`
/// without having reached it.
fn test_endpoint() -> Option<String> {
    live::url("KINESIS_TEST_ENDPOINT")
}

/// The production broker, connected in process by the suites that take any broker.
fn in_process() -> InProcessBroker<KinesisBroker> {
    InProcessBroker::new(KinesisBroker::new())
}

/// The production broker configured for the local stack.
fn live_broker(endpoint: &str) -> KinesisBroker {
    KinesisBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

/// Brokers for the local stack that share one lease store, the way replicas of a service share a
/// `DynamoDB` table: a second connection resumes from the checkpoints the first one left.
fn live_replicas(endpoint: String) -> impl Fn() -> KinesisBroker {
    let store: Arc<dyn LeaseStore> = Arc::new(MemoryLeaseStore::new());
    move || live_broker(&endpoint).lease_store(Arc::clone(&store))
}

/// The crate's descriptor with no start position: a subscription resumes from its checkpoint,
/// and a new one opens at the tip.
fn at_checkpoint(name: &str) -> KinesisStream {
    KinesisStream::new(name)
        .create_if_missing(1)
        .poll_interval(Duration::from_millis(200))
}

/// The crate's descriptor, creating the stream the suite names and reading it from its oldest
/// record: a live subscription finds its shards after `subscribe` returns, so a record published
/// right away is read from the horizon rather than raced against the tip.
fn from_horizon(name: &str) -> StartAt<KinesisStream, KinesisPosition> {
    StartAt::new(
        KinesisStream::new(name)
            .create_if_missing(1)
            .poll_interval(Duration::from_millis(200)),
        KinesisPosition::horizon(),
    )
}

/// The key a keyed publish carries, through the crate's per-message setting.
fn keyed(key: &[u8]) -> KinesisPublishOptions {
    KinesisPublishOptions {
        partition_key: Some(String::from_utf8_lossy(key).into_owned()),
    }
}

/// The per-message cases of the partition key: the policy's key when a call names none, the
/// call's own when it does, and a key the service refuses (257 characters) failing the publish.
fn partition_key_cases() -> OptionCases<KinesisPublishOptions, String> {
    OptionCases::new(POLICY_KEY.to_owned())
        .overrides(
            KinesisPublishOptions {
                partition_key: Some("conformance-call-key".to_owned()),
            },
            "conformance-call-key".to_owned(),
        )
        .overrides(KinesisPublishOptions::default(), POLICY_KEY.to_owned())
        .refuses(KinesisPublishOptions {
            partition_key: Some("k".repeat(257)),
        })
}

/// The partition key a delivery reports.
fn observed_key(msg: &KinesisMessage) -> String {
    String::from_utf8_lossy(IncomingMessage::partition_key(msg).unwrap_or_default()).into_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_conformance_suite() {
    harness::run_suite(KinesisBroker::new).await;
}

/// The lifecycle ladder in process: synchronous construction, the consuming transition, a
/// subscription opened through the crate's own descriptor, publishes it receives and settles from
/// other runtimes, the consuming `shutdown`, and - the part only a runtime check can hold - every
/// handle created before the shutdown erroring afterwards instead of succeeding against a closed
/// transport. Ahead of the ladder, every header shape a service writes must come back byte for
/// byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_lifecycle() {
    harness::lifecycle(
        in_process,
        |name| KinesisStream::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// A Kinesis stream addresses its own retry copies, so the descriptor promises that a publish to
/// the address it reports arrives at the subscription that reported it. That promise is the whole
/// deferred retry: an address reaching nothing would lose every delayed record silently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_reports_a_reachable_redelivery_address() {
    retry::redelivery_address(
        in_process,
        |name| KinesisStream::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The same promise for a bare name, the source `#[subscriber("orders")]` subscribes with: the
/// connected form declares a name the address of its own copies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_reports_a_reachable_redelivery_address_for_a_name() {
    retry::redelivery_address(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The in-process mode is `Seekable` and `Positioned` over its retained log, so it owes the same
/// contract the service does: a captured position redelivers exactly its record and the ordered
/// suffix after it, and a forward seek skips what was queued.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_seeking_suite() {
    capabilities::seeking(
        in_process,
        |name| KinesisStream::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// A position on a shard the stream does not have is refused, and the subscription stays where it
/// was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_refuses_a_seek_to_an_unknown_position() {
    capabilities::seeking_unknown_position(
        in_process,
        |name| KinesisStream::new(name),
        |connected| connected.publisher(),
        |_subject| KinesisPosition::sequence("shardId-000000000099", "1"),
    )
    .await;
}

/// The batch size is the one subscription parameter the framework carries down, so the size a
/// mount site names has to be the size a batch comes back at, and the elements of one batch settle
/// one by one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_batch_suite() {
    capabilities::batches(
        in_process,
        |name| KinesisStream::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// A batched subscription is seekable too, and a seek repositions its batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_batch_seeking_suite() {
    capabilities::batch_seeking(
        in_process,
        |name| KinesisStream::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// A record reports its partition key, and one key keeps its order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_keeps_keyed_order() {
    message_shape::keyed_order(
        in_process,
        &unique_subject("conformance.keyed"),
        |name| KinesisStream::new(name),
        |connected| connected.publisher(),
        |key, _headers| Some(keyed(key)),
    )
    .await;
}

/// The partition key a call names wins over the policy's for that call only, and a key the
/// service refuses fails the publish.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_resolves_publish_options() {
    message_shape::publish_options(
        in_process,
        &unique_subject("conformance.options"),
        |name| KinesisStream::new(name),
        KinesisPublish::default().partition_key(POLICY_KEY),
        partition_key_cases(),
        observed_key,
    )
    .await;
}

/// The generated document carries no credentials: the publish policy's bindings, and a server
/// configured from an endpoint URL that holds a user and a password.
#[cfg(feature = "asyncapi")]
#[test]
fn describes_without_credentials() {
    message_shape::publishes_without_credentials::<ConnectedKinesisBroker, _>(
        &KinesisPublish::default().partition_key("tenant-acme"),
        "hunter2",
    );
    message_shape::describes_addresses_without_credentials(
        |addrs| KinesisBroker::new().endpoint(addrs[0]),
        "https",
    );
}

/// The same ladder against the service, where the headers ride the record's data blob.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_passes_lifecycle() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    Box::pin(harness::lifecycle(
        move || live_broker(&endpoint),
        from_horizon,
        |connected| connected.publisher(),
    ))
    .await;
}

/// A shutdown finishes the publish handed to it right before, seen from a consumer of its own
/// that was already reading the stream.
///
/// A new consumer starts at the tip, so a record written while nobody read waits for a replica
/// that resumes from a checkpoint, never for a new subscription: the stream declares no backlog.
/// The observer keeps a lease store of its own, the way another service reads the same stream,
/// so it reads the shard beside the consumer under test instead of competing for its lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_flushes_on_shutdown() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    Box::pin(lifecycle::shutdown_flushes(
        move || live_broker(&endpoint),
        from_horizon,
        |connected| connected.publisher(),
        Backlog::Missed,
    ))
    .await;
}

/// What `ack` and `nack(false)` mean against the service, seen from a replica that resumes the
/// subscription, and that the in-process mode answers every settlement the same way. A requeue
/// answers unsupported on both, so the checks that wait for a redelivery end there. A new
/// subscription opens at the tip, so the record each check publishes right after subscribing is
/// the first one it reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_settles_like_the_in_process_mode() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    Box::pin(settlement::matches_in_process(
        live_replicas(endpoint),
        at_checkpoint,
        |connected| connected.publisher(),
        Duration::ZERO,
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_refuses_a_seek_to_an_unknown_position() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    Box::pin(capabilities::seeking_unknown_position(
        move || live_broker(&endpoint),
        from_horizon,
        |connected| connected.publisher(),
        |_subject| KinesisPosition::sequence("shardId-000000000099", "1"),
    ))
    .await;
}

/// The same promise against the service, where the address is a real stream name and the publish
/// is a real `PutRecord`: only a server shows that a copy published under the reported name is
/// picked up by the shard reader the descriptor opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_reports_a_reachable_redelivery_address() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    Box::pin(retry::redelivery_address(
        move || live_broker(&endpoint),
        from_horizon,
        |connected| connected.publisher(),
    ))
    .await;
}

/// The same batch contract against the service, where the size is a `GetRecords` limit rather
/// than a client-side cap: only a server can show that the reader asks for it and that a read
/// answering with fewer records still yields a batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_passes_batch_suite() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    Box::pin(capabilities::batches(
        move || live_broker(&endpoint),
        from_horizon,
        |connected| connected.publisher(),
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_passes_batch_seeking_suite() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    Box::pin(capabilities::batch_seeking(
        move || live_broker(&endpoint),
        from_horizon,
        |connected| connected.publisher(),
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_keeps_keyed_order() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    Box::pin(message_shape::keyed_order(
        move || live_broker(&endpoint),
        &unique_subject("conformance.keyed"),
        from_horizon,
        |connected| connected.publisher(),
        |key, _headers| Some(keyed(key)),
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kinesis_broker_resolves_publish_options() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    Box::pin(message_shape::publish_options(
        move || live_broker(&endpoint),
        &unique_subject("conformance.options"),
        from_horizon,
        KinesisPublish::default().partition_key(POLICY_KEY),
        partition_key_cases(),
        observed_key,
    ))
    .await;
}
