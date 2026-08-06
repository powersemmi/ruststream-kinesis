//! Repositioning a running subscription, and publishing with an explicit partition key.
//!
//! The `start_at` clause opens every shard at the trim horizon, so the retained backlog
//! replays first. The `Seek` parameter moves the subscription to the tip once the backlog is
//! no longer wanted, and the seeding publish rides the scope's `after_startup` hook, where
//! the publish policy is paired with the connected broker.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example kinesis_seek -- run`

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream, Seek};
use ruststream::{Headers, OutgoingMessage, Publisher, Seeker, subscriber};
use ruststream_kinesis::{
    KinesisBroker, KinesisError, KinesisPosition, KinesisPublish, KinesisSeeker, KinesisStream,
    PARTITION_KEY_HEADER,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Job {
    id: u64,
}

// --8<-- [start:seek]
/// Replays the retained backlog; on the marker record it abandons the rest of it and
/// follows the tip instead.
#[subscriber(KinesisStream::new("jobs"), start_at(KinesisPosition::horizon()))]
async fn replay(job: &Job, Seek(seeker): Seek<KinesisSeeker>) -> HandlerResult {
    if job.id == 999 {
        // `latest` is stream-wide: every shard of the subscription moves, including shards
        // discovered later.
        if seeker.seek(KinesisPosition::latest()).await.is_err() {
            return HandlerResult::retry();
        }
        return HandlerResult::Ack;
    }
    println!("replayed job {}", job.id);
    HandlerResult::Ack
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
            b.after_startup(
                KinesisPublish,
                async move |publisher| -> Result<(), KinesisError> {
                    let mut headers = Headers::new();
                    // The partition key decides the shard, and with it per-key ordering.
                    headers.insert(PARTITION_KEY_HEADER, "tenant-acme");
                    publisher
                        .publish(
                            OutgoingMessage::new("jobs", b"{\"id\":1}".as_slice())
                                .with_headers(headers),
                        )
                        .await
                },
            );
            b.include(replay);
        },
    )
}
// --8<-- [end:publish]
