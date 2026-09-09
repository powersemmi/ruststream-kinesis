//! Service-level tests on the framework's `TestApp` harness: real handlers, real dispatch, real
//! delivery contexts, no server.
//!
//! Every handler here is written exactly as a service would write it - the crate's own
//! descriptor in `#[subscriber(..)]`, the delivery context read by key - and mounts unchanged on
//! [`KinesisTestBroker`], whose retained log makes repositioning a real re-read rather than a
//! handle that accepts every seek.
#![cfg(feature = "testing")]

use std::future::{Future, ready};

use ruststream::testing::TestApp;
use ruststream_kinesis::prelude::*;
use ruststream_kinesis::testing::{KinesisTestBroker, KinesisTestPublish};
use ruststream_kinesis::{PARTITION_KEY_HEADER, SEQUENCE_HEADER, SHARD_HEADER};
use serde::{Deserialize, Serialize};

/// The id a producer uses to ask a consumer to abandon the rest of the retained backlog.
const MARKER: u64 = 999;

/// The destination is left to the call, so one model seeds every stream in this file.
#[derive(Debug, Clone, PartialEq, Eq, Outgoing, Serialize, Deserialize)]
struct Job {
    id: u64,
}

/// A receipt only ever goes to the receipts stream, so the type is where that stream is named.
#[derive(Debug, Clone, PartialEq, Eq, Outgoing, Serialize, Deserialize)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

// --8<-- [start:seek_handler]
/// Skips the rest of the backlog when the producer marks it: the seeker is a field of this
/// delivery's context, bound by the `SeekHandle` key.
#[subscriber(KinesisStream::new("jobs"))]
async fn work(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    if job.id == MARKER && seeker.seek(KinesisPosition::latest()).await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:seek_handler]

/// The position key and the delivery headers must name the same record. That is the contract a
/// batch body leans on, because a batch has no position of its own and reads it off the elements
/// instead; a handler can check it and settle by the answer.
#[subscriber(KinesisStream::new("audit"))]
async fn audit(
    _entry: &Job,
    ctx: &mut Context<'_, KinesisContext>,
    Ctx(at): Ctx<Position>,
) -> HandlerOutcome {
    let agrees = matches!(
        &at,
        KinesisPosition::Sequence { shard, sequence }
            if Some(shard.as_str()) == ctx.headers().get_str(SHARD_HEADER)
                && Some(sequence.as_str()) == ctx.headers().get_str(SEQUENCE_HEADER)
    );
    if agrees {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

/// A batch repositions the whole subscription once it is settled. The handle rides the batch
/// context, which carries no position - a batch spans many records.
#[subscriber(KinesisStream::new("batches"))]
async fn batches(batch: &[Job], ctx: &mut Context<'_, KinesisBatchContext>) -> HandlerOutcome {
    if batch.iter().any(|job| job.id == MARKER)
        && ctx
            .context(SeekHandle)
            .seek(KinesisPosition::latest())
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Replies with the file's nameless model, so the stream is the attribute's to name. The reply
/// publisher is paired from a policy, so a service never holds it and the mount site is the only
/// place its partition key can be named.
#[subscriber(KinesisStream::new("orders"), publish("receipts"))]
async fn confirm(order: &Job) -> Job {
    Job { id: order.id }
}

/// Replies with a type that names its own stream, so the attribute carries the bare clause.
#[subscriber(KinesisStream::new("orders"), publish)]
async fn issue(order: &Job) -> Receipt {
    Receipt { order: order.id }
}

/// The manual path's body: the same handler a service without the `macros` feature writes.
/// Nothing here awaits, so it is spelled as the plain `fn` the trait declares rather than an
/// `async fn` the compiler would have to build a state machine for.
struct Ledger;

impl Handle<Job> for Ledger {
    fn handle(
        &self,
        _entry: &Job,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> + Send {
        ready(Ok(()))
    }
}

/// Seeds a stream's retained log before the service exists, the way an external producer would.
async fn seed(broker: &KinesisTestBroker, stream: &str, ids: impl IntoIterator<Item = u64>) {
    let ingress = broker.publisher();
    for id in ids {
        ingress
            .message(&Job { id })
            .to(stream)
            .publish()
            .await
            .expect("the in-process publish succeeds");
    }
}

// --8<-- [start:seek_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_repositions_its_own_subscription_through_the_seek_handle() {
    let broker = KinesisTestBroker::new();
    // Published before the service exists, so the whole run is retained and the subscription's
    // start position is what decides whether it sees any of it.
    seed(&broker, "jobs", [1, MARKER, 2, 3]).await;

    let app = RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
        b.include(work.start_at(KinesisPosition::horizon()));
    });
    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.settle().await.expect("the replay settles");

    // Published after the seek, so it is ahead of the tip the marker jumped to.
    tb.broker::<KinesisTestBroker>()
        .message(&Job { id: 4 })
        .to("jobs")
        .publish()
        .await
        .expect("the publish succeeds");

    let handler = tb.broker::<KinesisTestBroker>();
    assert_eq!(
        handler.subscriber("jobs").received::<Job>(),
        vec![Job { id: 1 }, Job { id: MARKER }, Job { id: 4 }],
        "the seek to the tip must drop the backlog queued behind the marker",
    );
    handler
        .subscriber("jobs")
        .assert_called(3)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the harness shuts down");
}
// --8<-- [end:seek_test]

/// The partition key decides the shard, and a reply that needs per-key ordering needs one. The
/// mount site names it on the policy, and the record has to come out carrying it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mount_site_names_the_partition_key_of_a_reply() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisTestBroker::new(),
        |b| {
            b.include(confirm)
                .out(Reply, KinesisTestPublish::default())
                .partition_key("tenant-acme");
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");

    let broker = tb.broker::<KinesisTestBroker>();
    broker
        .message(&Job { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("the publish succeeds");

    broker
        .published::<Job>("receipts")
        .assert_called_once()
        .with_header(PARTITION_KEY_HEADER, "tenant-acme");

    tb.shutdown().await.expect("the harness shuts down");
}

/// A reply type that names its own stream lands on that stream, and the mount still decides how it
/// gets there: the policy the chain named is what the runtime pairs the reply publisher from, so
/// the record carries the partition key that policy was given.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_lands_on_the_stream_it_declares() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisTestBroker::new(),
        |b| {
            b.include(issue)
                .out(Reply, KinesisTestPublish::default())
                .partition_key("tenant-acme");
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");

    let broker = tb.broker::<KinesisTestBroker>();
    broker
        .message(&Job { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("the publish succeeds");

    broker.subscriber("orders").assert_called_once();
    broker
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { order: 1 })
        .with_header(PARTITION_KEY_HEADER, "tenant-acme");

    tb.shutdown().await.expect("the harness shuts down");
}

/// A reply type that names no stream takes the one the mount site names, which is the only place
/// the stream is written on this path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_type_without_a_stream_takes_the_mount_site_name() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisTestBroker::new(),
        |b| {
            b.include(confirm).out(Reply, KinesisTestPublish::default());
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");

    let broker = tb.broker::<KinesisTestBroker>();
    broker
        .message(&Job { id: 7 })
        .to("orders")
        .publish()
        .await
        .expect("the publish succeeds");

    broker.subscriber("orders").assert_called_once();
    broker
        .published::<Job>("receipts")
        .assert_called_once()
        .with(&Job { id: 7 });

    tb.shutdown().await.expect("the harness shuts down");
}

/// The descriptor is the crate's one way to name a stream, so it has to reach the manual path
/// too: a service without the `macros` feature hands it to the `subscriber(..)` constructor and
/// gets the same subscription the attribute would have mounted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_descriptor_names_a_stream_on_the_manual_path() {
    let app = RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        KinesisTestBroker::new(),
        |b| {
            b.include(subscriber(KinesisStream::new("ledger"), Ledger).build());
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");

    let broker = tb.broker::<KinesisTestBroker>();
    broker
        .message(&Job { id: 1 })
        .to("ledger")
        .publish()
        .await
        .expect("the publish succeeds");

    broker
        .subscriber("ledger")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the harness shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_position_key_names_the_record_the_delivery_headers_name() {
    let app = RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
        KinesisTestBroker::new(),
        |b| {
            b.include(audit);
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");

    let broker = tb.broker::<KinesisTestBroker>();
    for id in [1, 2, 3] {
        broker
            .message(&Job { id })
            .to("audit")
            .publish()
            .await
            .expect("the publish succeeds");
    }

    // The handler drops a record whose position disagrees with its headers, so an ack on all
    // three is the assertion.
    broker
        .subscriber("audit")
        .assert_called(3)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the harness shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_repositions_the_subscription_through_the_batch_context() {
    let broker = KinesisTestBroker::new();
    seed(&broker, "batches", [1, 2, MARKER, 3, 4]).await;

    let app = RustStream::new(AppInfo::new("batches", "0.1.0")).with_broker(broker, |b| {
        // The batch size is the one parameter the framework carries down to the broker; against
        // the service it becomes the `GetRecords` limit, and here the stand-in groups its
        // retained log by it.
        b.include(
            batches
                .start_at(KinesisPosition::horizon())
                .batch(nonzero!(3)),
        );
    });
    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.settle().await.expect("the replay settles");

    tb.broker::<KinesisTestBroker>()
        .message(&Job { id: 5 })
        .to("batches")
        .publish()
        .await
        .expect("the publish succeeds");

    let handler = tb.broker::<KinesisTestBroker>();
    assert_eq!(
        handler.subscriber("batches").received::<Job>(),
        vec![
            Job { id: 1 },
            Job { id: 2 },
            Job { id: MARKER },
            Job { id: 5 },
        ],
        "the batch's seek must drop the records queued behind it",
    );
    // Two batches: the backlog batch that carried the marker, and the one record published after
    // the subscription followed the tip.
    handler
        .subscriber("batches")
        .assert_called(2)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the harness shuts down");
}
