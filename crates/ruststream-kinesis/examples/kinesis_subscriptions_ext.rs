//! `KinesisSubscriberExt`: the subscription's read settings named on the mount chain, after the
//! framework's own steps and before `start_at(..)`. Each scenario is a test, and `cargo run`
//! starts the service against a local stack.
//!
//! ```text
//! cargo test --example kinesis_subscriptions_ext --features testing
//! ```

use std::time::Duration;

use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "audit")]
struct AuditEvent {
    id: u64,
}

// --8<-- [start:chain]
/// Archives every audit event the stream still retains.
#[subscriber(KinesisStream::new("audit"))]
async fn archive(event: &AuditEvent) -> HandlerOutcome {
    tracing::info!(event.id, "archived");
    HandlerOutcome::ack()
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
        // The framework's step first, this crate's settings next, the start position last.
        b.include(
            archive
                .workers(nonzero!(4))
                .poll_interval(Duration::from_millis(250))
                .create_if_missing(1)
                .start_at(KinesisPosition::horizon()),
        );
    })
}
// --8<-- [end:chain]

#[ruststream::app]
fn app() -> impl App {
    service(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_chained_settings_mount_and_deliver() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        for id in [1, 2] {
            tb.broker::<KinesisBroker>()
                .message(&AuditEvent { id })
                .publish()
                .await?;
        }

        tb.broker::<KinesisBroker>()
            .subscriber("audit")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
