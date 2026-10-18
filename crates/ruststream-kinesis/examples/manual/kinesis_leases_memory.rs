//! The service of `examples/kinesis_leases_memory.rs` written without the macros:
//! `MemoryLeaseStore` keeps the checkpoints of one instance in its process.
//!
//! ```text
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
//!     cargo test --example manual_kinesis_leases_memory --features testing -- --test-threads=1
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::sync::Arc;

use ruststream::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use ruststream_kinesis::MemoryLeaseStore;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct AuditEvent {
    id: u64,
}

// `#[outgoing(name = "audit-trail")]` by hand: the type fixes its destination.
impl OutgoingDestination for AuditEvent {
    type Form = FixedName;

    const DESTINATION: &'static str = "audit-trail";
}

impl MessageHeaders for AuditEvent {
    type Contract = NoHeaders;
}

// --8<-- [start:memory]
/// Records each audit event. One instance of the service reads every shard.
struct Record;

impl Handle<AuditEvent> for Record {
    fn handle(
        &self,
        event: &AuditEvent,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.id, "recorded");
        ready(Ok(()))
    }
}

fn service(broker: KinesisBroker) -> RustStream {
    // The store a broker uses when it names none, named here: its checkpoints live in this
    // process and go with it.
    let leases = Arc::new(MemoryLeaseStore::new());
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker.lease_store(leases), |b| {
        b.include(subscriber(KinesisStream::new("audit-trail"), Record).build());
    })
}
// --8<-- [end:memory]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let app = service(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
    );
    app.run().await?;
    Ok(())
}

#[cfg(test)]
#[path = "../support/kinesis_live.rs"]
mod support;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_restarted_process_opens_at_the_tip() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = support::endpoint() else {
            return Ok(());
        };
        let config = support::config(&endpoint).await;
        support::ensure_stream(&config, "audit-trail", 1).await;

        let first = TestApp::start_live(service(support::broker(&endpoint))).await?;
        first
            .broker::<KinesisBroker>()
            .message(&AuditEvent { id: 1 })
            .publish()
            .await?;
        first
            .broker::<KinesisBroker>()
            .subscriber("audit-trail")
            .assert_called_once();
        first.shutdown().await?;

        // Written while the service is down.
        support::put_raw(&config, "audit-trail", r#"{"id":2}"#).await;

        let second = TestApp::start_live(service(support::broker(&endpoint))).await?;
        second
            .broker::<KinesisBroker>()
            .message(&AuditEvent { id: 3 })
            .publish()
            .await?;

        // The new process has no checkpoint, so it opened at the tip and skipped event 2.
        assert_eq!(
            second
                .broker::<KinesisBroker>()
                .subscriber("audit-trail")
                .received::<AuditEvent>(),
            [AuditEvent { id: 3 }],
        );
        second.shutdown().await?;
        Ok(())
    }
}
