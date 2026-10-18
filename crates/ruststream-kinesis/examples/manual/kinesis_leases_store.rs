//! The service of `examples/kinesis_leases_store.rs` written without the macros: the lease store a
//! broker is handed decides which instance reads which shard.
//!
//! ```text
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
//!     cargo test --example manual_kinesis_leases_store --features testing -- --test-threads=1
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::sync::Arc;

use ruststream::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use ruststream_kinesis::prelude::*;
use ruststream_kinesis::{LeaseStore, MemoryLeaseStore};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct StockChange {
    sku: u64,
    delta: i64,
}

// `#[outgoing(name = "inventory")]` by hand: the type fixes its destination.
impl OutgoingDestination for StockChange {
    type Form = FixedName;

    const DESTINATION: &'static str = "inventory";
}

impl MessageHeaders for StockChange {
    type Contract = NoHeaders;
}

// --8<-- [start:store]
/// Applies each stock change. Every instance of the service runs this handler; the lease store
/// decides which instance reads which shard.
struct Apply;

impl Handle<StockChange> for Apply {
    fn handle(
        &self,
        change: &StockChange,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(change.sku, change.delta, "stock changed");
        ready(Ok(()))
    }
}

fn service(broker: KinesisBroker, leases: Arc<dyn LeaseStore>) -> RustStream {
    RustStream::new(AppInfo::new("inventory", "0.1.0")).with_broker(
        broker.lease_store(leases),
        |b| {
            b.include(subscriber(KinesisStream::new("inventory"), Apply).build());
        },
    )
}
// --8<-- [end:store]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let broker = KinesisBroker::new()
        .endpoint("http://localhost:4566")
        .test_credentials()
        .region("us-east-1");
    let app = service(broker, Arc::new(MemoryLeaseStore::new()));
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
    async fn one_instance_reads_the_shard_while_its_lease_lives() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = support::endpoint() else {
            return Ok(());
        };
        support::ensure_stream(&support::config(&endpoint).await, "inventory", 1).await;
        let leases: Arc<dyn LeaseStore> = Arc::new(MemoryLeaseStore::new());

        let first = TestApp::start_live(service(
            support::broker(&endpoint).owner_id("inventory-a"),
            Arc::clone(&leases),
        ))
        .await?;
        let second = TestApp::start_live(service(
            support::broker(&endpoint).owner_id("inventory-b"),
            Arc::clone(&leases),
        ))
        .await?;

        for sku in 1..=3 {
            first
                .broker::<KinesisBroker>()
                .message(&StockChange { sku, delta: -1 })
                .publish()
                .await?;
        }

        first
            .broker::<KinesisBroker>()
            .subscriber("inventory")
            .assert_called(3);
        second
            .broker::<KinesisBroker>()
            .subscriber("inventory")
            .assert_not_called();
        first.shutdown().await?;
        second.shutdown().await?;
        Ok(())
    }
}
