//! The service of `examples/kinesis_seek.rs` written without the macros: a body typed over
//! `KinesisContext` reads the seek handle off its context, and the startup hook seeds the stream
//! with a partition key on the publish.
//!
//! ```text
//! cargo test --example manual_kinesis_seek --features testing
//! cargo run --example manual_kinesis_seek --features testing -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The job a producer writes to say the rest of the backlog is no longer wanted.
const MARKER: u64 = 999;

// --8<-- [start:seek]
/// Replays the retained backlog; on the marker job it abandons the rest of it and follows the
/// tip instead.
struct Replay;

impl Handle<Job, (), (), KinesisContext> for Replay {
    async fn handle(
        &self,
        job: &Job,
        _outs: &(),
        ctx: &mut Context<'_, KinesisContext>,
    ) -> Result<(), HandlerOutcome> {
        if job.id == MARKER {
            ctx.context(SeekHandle)
                .seek(KinesisPosition::latest())
                .await
                .map_err(|_| HandlerOutcome::retry())?;
        }
        tracing::info!(job.id, "job replayed");
        Ok(())
    }
}
// --8<-- [end:seek]

// --8<-- [start:publish]
fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.after_startup(Publish::default(), async move |publisher| {
            // The partition key picks the shard, and with it the order this job keeps.
            publisher
                .message(&Job { id: 1 })
                .to("jobs")
                .partition_key("tenant-acme")
                .publish()
                .await
        });
        b.include(
            subscriber(KinesisStream::new("jobs"), Replay)
                .build()
                .start_at(KinesisPosition::horizon()),
        );
    })
}
// --8<-- [end:publish]

fn main() -> ExitCode {
    run_main(app)
}

/// A job; the stream is the call's to name.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
struct Job {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Job, MARKER, app};

    #[tokio::test]
    async fn the_marker_skips_the_rest_of_the_backlog() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // The seed job of the startup hook.
        tb.settle().await?;

        // All four are in the stream before the handler reads the marker.
        let broker = tb.broker::<KinesisBroker>();
        let backlog = [2, MARKER, 3, 4].map(|id| Job { id });
        for published in join_all(
            backlog
                .iter()
                .map(|job| broker.message(job).to("jobs").publish()),
        )
        .await
        {
            published?;
        }
        broker.message(&Job { id: 5 }).to("jobs").publish().await?;

        let replayed: Vec<u64> = broker
            .subscriber("jobs")
            .received::<Job>()
            .iter()
            .map(|job| job.id)
            .collect();
        assert_eq!(replayed, [1, 2, MARKER, 5]);
        tb.shutdown().await?;
        Ok(())
    }
}
