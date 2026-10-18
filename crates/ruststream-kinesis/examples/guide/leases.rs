//! The service of the Several instances guide: every instance runs the same handler, and a lease
//! table in `DynamoDB` gives each shard of the stream to one of them.
//!
//! ```text
//! docker compose up -d
//! AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
//!     AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//!     cargo run --example guide_leases --features testing,dynamodb-lease
//! ```

use std::error::Error;
use std::sync::Arc;

use aws_config::{BehaviorVersion, SdkConfig};
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:service]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "warehouse-orders")]
struct Order {
    id: u64,
    sku: String,
}

/// Picks each order. Every instance runs this handler, and each shard is read by the one instance
/// that holds its lease.
#[subscriber(KinesisStream::new("warehouse-orders").create_if_missing(2))]
async fn pick(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, order.sku, "order picked");
    HandlerOutcome::ack()
}

/// The lease store and the broker share one AWS configuration: one account, one region.
fn service(config: &SdkConfig, table: &str) -> RustStream {
    let leases = Arc::new(DynamoLeaseStore::new(config, table));
    let broker = KinesisBroker::from_config(config.clone()).lease_store(leases);
    RustStream::new(AppInfo::new("warehouse", "0.1.0")).with_broker(broker, |b| {
        b.include(pick);
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    service(&config, "warehouse-leases").run().await?;
    Ok(())
}
// --8<-- [end:service]

#[cfg(test)]
#[path = "../support/kinesis_live.rs"]
mod support;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use aws_config::Region;
    use aws_sdk_kinesis::config::{Credentials, SharedCredentialsProvider};
    use ruststream::HeaderMap;
    use ruststream::testing::TestApp;
    use ruststream_kinesis::PARTITION_KEY_HEADER;

    use super::*;

    /// A configuration built without I/O. The in-process Kinesis signs nothing and consults no
    /// lease store, so the values only have to be complete.
    fn config() -> SdkConfig {
        SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "test", "test", None, None, "tests",
            )))
            .build()
    }

    /// An order, keyed by its SKU.
    async fn place(tb: &TestApp<()>, id: u64) -> Result<(), Box<dyn Error>> {
        let mut key = HeaderMap::new();
        key.insert(PARTITION_KEY_HEADER, "sku-7");
        tb.broker::<KinesisBroker>()
            .message(&Order {
                id,
                sku: "sku-7".to_owned(),
            })
            .with_headers(key)
            .publish()
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_order_is_picked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(&config(), "warehouse-leases")).await?;

        place(&tb, 1).await?;

        tb.broker::<KinesisBroker>()
            .subscriber("warehouse-orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    /// Two instances on one table: the first holds the shards, and when it stops the second takes
    /// them over and resumes after the first one's checkpoint.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_second_instance_takes_over_where_the_first_stopped() -> Result<(), Box<dyn Error>>
    {
        let Some(endpoint) = support::endpoint() else {
            return Ok(());
        };
        let config = support::config(&endpoint).await;
        let table = support::unique("warehouse-leases");
        support::create_lease_table(&config, &table).await;
        // A takeover waits for the next shard listing, ten seconds at most.
        let within = Duration::from_secs(30);

        let first = TestApp::start_live_within(service(&config, &table), within).await?;
        let second = TestApp::start_live_within(service(&config, &table), within).await?;

        place(&first, 1).await?;
        first
            .broker::<KinesisBroker>()
            .subscriber("warehouse-orders")
            .assert_called_once();
        second
            .broker::<KinesisBroker>()
            .subscriber("warehouse-orders")
            .assert_not_called();
        first.shutdown().await?;

        place(&second, 2).await?;
        let picked: Vec<u64> = second
            .broker::<KinesisBroker>()
            .subscriber("warehouse-orders")
            .received::<Order>()
            .into_iter()
            .map(|order| order.id)
            .collect();
        assert_eq!(picked, [2]);
        second.shutdown().await?;
        Ok(())
    }
}
