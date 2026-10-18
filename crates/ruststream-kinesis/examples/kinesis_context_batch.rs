//! The batch context: a batch body reaches the subscription's seek handle through
//! `KinesisBatchContext`, and moves the subscription once the batch has settled.
//!
//! ```text
//! cargo test --example kinesis_context_batch --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

/// The job a producer writes to say the rest of the backlog is no longer wanted.
const MARKER: u64 = 999;

// --8<-- [start:batch]
/// Drains jobs a batch at a time; a batch holding the marker job moves the subscription to the
/// tip, so the backlog behind it is skipped.
#[subscriber(KinesisStream::new("jobs"))]
async fn drain(jobs: &[Job], ctx: &mut Context<'_, KinesisBatchContext>) -> HandlerOutcome {
    tracing::info!(count = jobs.len(), "jobs drained");
    let _ = ctx;
    HandlerOutcome::ack()
}
// --8<-- [end:batch]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(drain.batch(nonzero!(2)));
    })
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "jobs")]
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
    async fn the_batch_after_the_marker_starts_at_the_tip() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // All three are in the stream before the first batch is read.
        let broker = tb.broker::<KinesisBroker>();
        let backlog = [1, MARKER, 2].map(|id| Job { id });
        for published in join_all(backlog.iter().map(|job| broker.message(job).publish())).await {
            published?;
        }
        broker.message(&Job { id: 3 }).publish().await?;

        let drained: Vec<Vec<u64>> = broker
            .subscriber("jobs")
            .batches::<Job>()
            .iter()
            .map(|batch| batch.iter().map(|job| job.id).collect())
            .collect();
        assert_eq!(drained, [vec![1, MARKER], vec![3]]);
        tb.shutdown().await?;
        Ok(())
    }
}
