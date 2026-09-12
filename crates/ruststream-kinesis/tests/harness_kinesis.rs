//! Service-level tests on the framework's `TestApp` harness: real handlers, real dispatch, real
//! delivery contexts, no server.
//!
//! Every handler here is written exactly as a service would write it - the crate's own
//! descriptor in `#[subscriber(..)]`, the delivery context read by key - and mounts unchanged on
//! [`KinesisTestBroker`], whose retained log makes repositioning a real re-read rather than a
//! handle that accepts every seek.
#![cfg(feature = "testing")]

use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ruststream::testing::{TestApp, TestError};
use ruststream_kinesis::prelude::*;
use ruststream_kinesis::testing::KinesisTestBroker;
use ruststream_kinesis::{PARTITION_KEY_HEADER, SEQUENCE_HEADER, SHARD_HEADER};
use serde::{Deserialize, Serialize};
use tokio::sync::Barrier;
use tokio::time::timeout;

/// The id a producer uses to ask a consumer to abandon the rest of the retained backlog.
const MARKER: u64 = 999;

/// How long a settle may take before the test calls it a hang rather than slow progress. Long
/// enough that a loaded machine never reaches it, short enough to fail inside a CI job.
const QUIESCENCE_BUDGET: Duration = Duration::from_secs(10);

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

/// The descriptor a service ships carries the settings that price a real read, and it mounts
/// here carrying them.
#[subscriber(
    KinesisStream::new("priced")
        .poll_interval(Duration::from_millis(250))
        .create_if_missing(2)
)]
async fn priced(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// The same two settings, named at the mount site through the crate's settings chain instead.
#[subscriber(KinesisStream::new("chained"))]
async fn chained(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Named no start position, so it opens where a shard without a checkpoint opens: at the tip.
#[subscriber(KinesisStream::new("tip"))]
async fn from_the_tip(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Opened at a wall-clock instant instead of an end of the log.
#[subscriber(KinesisStream::new("since"))]
async fn since(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// A descriptor that names no stream. Nothing can subscribe to it, on the stand-in or against
/// the service.
#[subscriber(KinesisStream::new(""))]
async fn unnamed(_job: &Job) -> HandlerOutcome {
    HandlerOutcome::ack()
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

/// What the two rewinding deliveries share: a budget, so only the first pair rewinds and the
/// replay it asks for does not rewind again, and a rendezvous that holds both of them in flight
/// until both have sought.
#[derive(Debug, Clone)]
struct Rewind {
    budget: Arc<AtomicUsize>,
    both_sought: Arc<Barrier>,
}

/// The application state of the worker-pool mount below.
#[derive(Debug, Clone, FromRef)]
struct Lanes {
    rewind: Rewind,
}

/// Rewinds to the horizon on each of its first two deliveries and waits there for the other one.
///
/// A pool polls its subscription again only when a worker frees a slot, so holding both
/// deliveries here until both have sought is what stacks the second reposition on top of a first
/// one the subscription never applied.
#[subscriber(KinesisStream::new("lanes"))]
async fn lanes(
    _job: &Job,
    Ctx(seeker): Ctx<SeekHandle>,
    State(rewind): State<Rewind>,
) -> HandlerOutcome {
    let rewinds = rewind
        .budget
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
            left.checked_sub(1)
        })
        .is_ok();
    if !rewinds {
        return HandlerOutcome::ack();
    }
    if seeker.seek(KinesisPosition::horizon()).await.is_err() {
        return HandlerOutcome::retry();
    }
    rewind.both_sought.wait().await;
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

/// The partition key decides the shard, and a reply that needs per-key ordering needs one. The
/// mount site names it on the policy, and the record has to come out carrying it.
///
/// The policy is the crate's own - the type a production routes file names - so this mount is
/// the mount the service ships, character for character.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mount_site_names_the_partition_key_of_a_reply() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisTestBroker::new(),
        |b| {
            b.include(confirm)
                .out(Reply, Publish::default())
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
                .out(Reply, Publish::default())
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
            b.include(confirm).out(Reply, Publish::default());
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

/// A mount that names no publisher replies through the broker's default policy, and the
/// stand-in's default is the crate's own: the reply still comes out on the stream the handler
/// named, carrying the spread key a policy without one gives every record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_without_a_named_publisher_goes_through_the_default_policy() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisTestBroker::new(),
        |b| {
            b.include(confirm);
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");

    let broker = tb.broker::<KinesisTestBroker>();
    broker
        .message(&Job { id: 8 })
        .to("orders")
        .publish()
        .await
        .expect("the publish succeeds");

    broker
        .published::<Job>("receipts")
        .assert_called_once()
        .with(&Job { id: 8 });

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

/// The settings on the descriptor are the service's, and a unit test must not have to strip
/// them out to mount it: the stand-in has no read to price and no stream to create, so it
/// ignores them and delivers all the same. Both spellings mount - the attribute carrying the
/// settings on the descriptor, and the chain naming them at the mount site.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_descriptor_carrying_its_own_settings_mounts_on_the_stand_in() {
    let app = RustStream::new(AppInfo::new("settings", "0.1.0")).with_broker(
        KinesisTestBroker::new(),
        |b| {
            b.include(priced);
            // The framework's own step comes first and this crate's chain after it, which is the
            // order a mount site writes them in.
            b.include(
                chained
                    .workers(nonzero!(2))
                    .poll_interval(Duration::from_millis(250))
                    .create_if_missing(2),
            );
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");

    let broker = tb.broker::<KinesisTestBroker>();
    for stream in ["priced", "chained"] {
        broker
            .message(&Job { id: 1 })
            .to(stream)
            .publish()
            .await
            .expect("the publish succeeds");
        broker
            .subscriber(stream)
            .assert_called_once()
            .with(&Job { id: 1 })
            .settled(HandlerOutcome::ack());
    }

    tb.shutdown().await.expect("the harness shuts down");
}

/// A mount that names no position opens at the tip, which is where a shard without a checkpoint
/// opens: the backlog a producer left behind stays unread, and only what arrives afterwards
/// reaches the handler. Checkpoint resume itself is a server property and is covered live; what
/// a handler can observe here is the start the descriptor defaults to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mount_without_a_start_position_opens_at_the_tip() {
    let broker = KinesisTestBroker::new();
    seed(&broker, "tip", [1, 2]).await;

    let app = RustStream::new(AppInfo::new("tip", "0.1.0")).with_broker(broker, |b| {
        b.include(from_the_tip);
    });
    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.settle().await.expect("there is nothing to replay");

    tb.broker::<KinesisTestBroker>()
        .message(&Job { id: 3 })
        .to("tip")
        .publish()
        .await
        .expect("the publish succeeds");

    assert_eq!(
        tb.broker::<KinesisTestBroker>()
            .subscriber("tip")
            .received::<Job>(),
        vec![Job { id: 3 }],
        "a subscription that named no position must not replay the retained backlog",
    );

    tb.shutdown().await.expect("the harness shuts down");
}

/// The third stream-wide position: a timestamp opens the subscription at the first record from
/// that instant, and the epoch is before every record the log retains.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timestamp_position_opens_the_subscription_over_the_retained_log() {
    let broker = KinesisTestBroker::new();
    seed(&broker, "since", [1, 2]).await;

    let app = RustStream::new(AppInfo::new("since", "0.1.0")).with_broker(broker, |b| {
        b.include(since.start_at(KinesisPosition::timestamp(0)));
    });
    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.settle().await.expect("the replay settles");

    assert_eq!(
        tb.broker::<KinesisTestBroker>()
            .subscriber("since")
            .received::<Job>(),
        vec![Job { id: 1 }, Job { id: 2 }],
        "the epoch precedes every retained record, so the whole log replays",
    );

    tb.shutdown().await.expect("the harness shuts down");
}

/// A descriptor the service would reject is rejected here too, at the same point: the mount
/// fails when the subscription opens, rather than mounting something that can never deliver.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_invalid_descriptor_fails_the_mount_on_the_stand_in() {
    let app = RustStream::new(AppInfo::new("unnamed", "0.1.0")).with_broker(
        KinesisTestBroker::new(),
        |b| {
            b.include(unnamed);
        },
    );

    let Err(err) = TestApp::start(app).await else {
        panic!("a descriptor that names no stream must not mount");
    };
    assert!(matches!(err, TestError::Subscribe(_)), "got {err}");
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

/// A pool runs several of one subscription's deliveries at once, so two handlers can seek before
/// the subscription polls again and the second reposition replaces a first one it never applied.
/// The replaced reposition must not leave its replay counted in flight: nothing would ever settle
/// those deliveries, and the harness would wait for a quiescence that cannot arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reposition_replaced_before_it_is_applied_leaves_nothing_in_flight() {
    let broker = KinesisTestBroker::new();
    seed(&broker, "lanes", [1, 2, 3, 4, 5, 6]).await;

    let app = RustStream::new(AppInfo::new("lanes", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(Lanes {
                rewind: Rewind {
                    budget: Arc::new(AtomicUsize::new(2)),
                    both_sought: Arc::new(Barrier::new(2)),
                },
            })
        })
        .with_broker(broker, |b| {
            b.include(
                lanes
                    .start_at(KinesisPosition::horizon())
                    .workers(nonzero!(2)),
            );
        });
    let tb = TestApp::start(app).await.expect("the harness starts");

    // Bounded on purpose: a discarded reposition leaves its replay counted in flight forever, and
    // an unbounded wait would report that as a CI job that timed out with nothing to read.
    timeout(QUIESCENCE_BUDGET, tb.settle())
        .await
        .expect("the reaction reaches quiescence")
        .expect("the replay settles");

    // The rewinding pair, then the whole retained log once the surviving reposition is applied.
    tb.broker::<KinesisTestBroker>()
        .subscriber("lanes")
        .assert_called(8)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the harness shuts down");
}
