//! A lease store: what decides which instance reads a shard and where each shard resumes. The
//! scenario runs against a stand, since the in-process transport consults no store.
//!
//! ```text
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
//!     cargo test --example kinesis_leases_store --features testing -- --test-threads=1
//! ```

use std::sync::Arc;

use ruststream_kinesis::prelude::*;
use ruststream_kinesis::{LeaseStore, MemoryLeaseStore};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "inventory")]
struct StockChange {
    sku: u64,
    delta: i64,
}

// --8<-- [start:store]
/// Applies each stock change. Every instance of the service runs this handler; the lease store
/// decides which instance reads which shard.
#[subscriber(KinesisStream::new("inventory"))]
async fn apply(change: &StockChange) -> HandlerOutcome {
    tracing::info!(change.sku, change.delta, "stock changed");
    HandlerOutcome::ack()
}

fn service(broker: KinesisBroker, leases: Arc<dyn LeaseStore>) -> RustStream {
    RustStream::new(AppInfo::new("inventory", "0.1.0")).with_broker(
        broker.lease_store(leases),
        |b| {
            b.include(apply);
        },
    )
}
// --8<-- [end:store]

#[ruststream::app]
fn app() -> impl App {
    let broker = KinesisBroker::new()
        .endpoint("http://localhost:4566")
        .test_credentials()
        .region("us-east-1");
    service(broker, Arc::new(MemoryLeaseStore::new()))
}

#[cfg(test)]
#[path = "support/kinesis_live.rs"]
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
