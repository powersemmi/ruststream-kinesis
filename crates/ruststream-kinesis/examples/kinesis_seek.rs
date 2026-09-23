//! Repositioning a running subscription, and publishing with an explicit partition key.
//!
//! The `start_at` clause opens every shard at the trim horizon, so the retained backlog
//! replays first. The `SeekHandle` context key moves the subscription to the tip once the backlog
//! is no longer wanted, and the seeding publish rides the scope's `after_startup` hook, naming its
//! partition key with the `partition_key(..)` step on the publish builder.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example kinesis_seek -- run`

use ruststream::{Outgoing, Serialized};
use ruststream_kinesis::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

/// The seeding record is already encoded, so it declares itself serialized: it reaches the
/// stream byte for byte, and the generated document still names it.
#[derive(Outgoing, Serialized)]
struct SeedJob(Vec<u8>);

// --8<-- [start:seek]
/// Replays the retained backlog; on the marker record it abandons the rest of it and
/// follows the tip instead. The seeker is a field of the delivery context, read by the
/// `SeekHandle` key.
#[subscriber(KinesisStream::new("jobs"), start_at(KinesisPosition::horizon()))]
async fn replay(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    if job.id == 999 {
        // `latest` is stream-wide: every shard of the subscription moves, including shards
        // discovered later.
        if seeker.seek(KinesisPosition::latest()).await.is_err() {
            return HandlerOutcome::retry();
        }
        return HandlerOutcome::ack();
    }
    println!("replayed job {}", job.id);
    HandlerOutcome::ack()
}
// --8<-- [end:seek]

// --8<-- [start:publish]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
        |b| {
            b.after_startup(Publish::default(), async move |publisher| {
                // The partition key decides the shard, and with it per-key ordering. It is a
                // setting of this one record, so it is a step on the publish rather than a
                // publisher of its own.
                publisher
                    .message(&SeedJob(br#"{"id":1}"#.to_vec()))
                    .to("jobs")
                    .partition_key("tenant-acme")
                    .publish()
                    .await
            });
            b.include(replay);
        },
    )
}
// --8<-- [end:publish]
