//! Service-level tests on the framework's `TestApp` harness: real handlers, real dispatch, real
//! delivery contexts, no server.
//!
//! Every handler here is written exactly as a service would write it - the crate's own
//! descriptor in `#[subscriber(..)]`, the delivery context read by key - and mounts unchanged on
//! [`KinesisTestBroker`], whose retained log makes repositioning a real re-read rather than a
//! handle that accepts every seek.
#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_kinesis::prelude::*;
use ruststream_kinesis::testing::KinesisTestBroker;
use ruststream_kinesis::{SEQUENCE_HEADER, SHARD_HEADER};
use serde::{Deserialize, Serialize};

/// The id a producer uses to ask a consumer to abandon the rest of the retained backlog.
const MARKER: u64 = 999;

/// The destination is left to the call, so one model seeds every stream in this file.
#[derive(Debug, Clone, PartialEq, Eq, Outgoing, Serialize, Deserialize)]
struct Job {
    id: u64,
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
