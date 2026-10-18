//! `start_at`: the audit projection is rebuilt from everything the stream retains, on every
//! startup. The in-process test runs everywhere; the live one restarts the service against the
//! stand at `KINESIS_TEST_ENDPOINT`, and skips without it.
//!
//! ```text
//! cargo test --example kinesis_positions_start_at --features testing
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example kinesis_positions_start_at --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

#[cfg(test)]
#[path = "support/kinesis_stand.rs"]
mod stand;

// --8<-- [start:start_at]
/// Rebuilds the audit projection from the oldest retained entry on every startup.
#[subscriber(
    KinesisStream::new("ledger.audit"),
    start_at(KinesisPosition::horizon())
)]
async fn project(entry: &AuditEntry) -> HandlerOutcome {
    tracing::info!(entry.id, "entry projected");
    HandlerOutcome::ack()
}
// --8<-- [end:start_at]

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
        b.include(project);
    })
}

#[ruststream::app]
fn app() -> impl App {
    service(KinesisBroker::new())
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger.audit")]
struct AuditEntry {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{AuditEntry, app, service, stand};

    #[tokio::test]
    async fn an_entry_is_projected() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&AuditEntry { id: 1 })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("ledger.audit")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_startup_replays_the_retained_log() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = stand::endpoint() else {
            return Ok(());
        };
        stand::recreate_stream(&stand::config(&endpoint).await, "ledger.audit").await;

        // The first run reads the entry and checkpoints it.
        let first =
            TestApp::start_live_within(service(stand::broker(&endpoint)), stand::START).await?;
        first
            .broker::<KinesisBroker>()
            .message(&AuditEntry { id: 1 })
            .publish()
            .await?;
        first.shutdown().await?;

        let tb =
            TestApp::start_live_within(service(stand::broker(&endpoint)), stand::START).await?;
        tb.broker::<KinesisBroker>()
            .message(&AuditEntry { id: 2 })
            .publish()
            .await?;

        // A live publish returns once the subscription has handled as many records as the test
        // published, so the first of them is what decides: the entry the first run handled.
        let projected = tb
            .broker::<KinesisBroker>()
            .subscriber("ledger.audit")
            .received::<AuditEntry>();
        assert_eq!(projected.first(), Some(&AuditEntry { id: 1 }));
        tb.shutdown().await?;
        Ok(())
    }
}
