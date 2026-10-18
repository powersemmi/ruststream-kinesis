//! Sharing the shards between service instances through a `DynamoDB` lease store: one instance
//! reads a shard at a time, and the checkpoints survive a restart. The scenario runs against a
//! stand with `DynamoDB`, since the in-process transport consults no store.
//!
//! ```text
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
//!     cargo test --example kinesis_leases --features testing,dynamodb-lease -- --test-threads=1
//! ```

use std::sync::Arc;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_kinesis::config::{Credentials, SharedCredentialsProvider};
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "fulfilment")]
struct Order {
    id: u64,
}

// --8<-- [start:leases]
/// Fulfils each order. Every instance runs this handler, and each shard is read by the one
/// instance that holds its lease in the table.
#[subscriber(KinesisStream::new("fulfilment"))]
async fn fulfil(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "fulfilled");
    HandlerOutcome::ack()
}

/// The lease store and the broker share one AWS configuration: one account, one region.
fn broker(config: &SdkConfig, table: &str) -> KinesisBroker {
    let leases = Arc::new(DynamoLeaseStore::new(config, table));
    KinesisBroker::from_config(config.clone()).lease_store(leases)
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(broker, |b| {
        b.include(fulfil);
    })
}
// --8<-- [end:leases]

#[ruststream::app]
fn app() -> impl App {
    let config = SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url("http://localhost:4566")
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            "test",
            "test",
            None,
            None,
            "local-stack",
        )))
        .build();
    service(broker(&config, "fulfilment-leases"))
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
    async fn a_restarted_instance_resumes_from_the_table() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = support::endpoint() else {
            return Ok(());
        };
        let config = support::config(&endpoint).await;
        let table = support::unique("fulfilment-leases");
        support::create_lease_table(&config, &table).await;
        support::ensure_stream(&config, "fulfilment", 1).await;
        // A restart of one instance keeps its owner id.
        let instance = || broker(&config, &table).owner_id("fulfilment-1");

        let first = TestApp::start_live(service(instance())).await?;
        first
            .broker::<KinesisBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;
        first
            .broker::<KinesisBroker>()
            .subscriber("fulfilment")
            .assert_called_once();
        first.shutdown().await?;

        // Written while the service is down.
        support::put_raw(&config, "fulfilment", r#"{"id":2}"#).await;

        let second = TestApp::start_live(service(instance())).await?;
        second
            .broker::<KinesisBroker>()
            .message(&Order { id: 3 })
            .publish()
            .await?;

        // The table kept the checkpoint after order 1, so the restarted instance reads order 2
        // before the order published after the restart.
        let read = second
            .broker::<KinesisBroker>()
            .subscriber("fulfilment")
            .received::<Order>();
        assert_eq!(read[0], Order { id: 2 });
        second.shutdown().await?;
        Ok(())
    }
}
