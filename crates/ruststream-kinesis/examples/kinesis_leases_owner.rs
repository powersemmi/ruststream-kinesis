//! `owner_id`: the name this instance holds its leases under in the lease table. The scenario
//! runs against a stand with `DynamoDB`.
//!
//! ```text
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
//!     cargo test --example kinesis_leases_owner --features testing,dynamodb-lease -- --test-threads=1
//! ```

use std::env;
use std::sync::Arc;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_kinesis::config::{Credentials, SharedCredentialsProvider};
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "dispatch")]
struct Pickup {
    id: u64,
}

#[subscriber(KinesisStream::new("dispatch"))]
async fn assign(pickup: &Pickup) -> HandlerOutcome {
    tracing::info!(pickup.id, "courier assigned");
    HandlerOutcome::ack()
}

// --8<-- [start:owner]
/// `instance` names this instance in the lease table: the same name after a restart, a
/// different one on every other instance.
fn broker(config: &SdkConfig, table: &str, instance: &str) -> KinesisBroker {
    let leases = Arc::new(DynamoLeaseStore::new(config, table));
    KinesisBroker::from_config(config.clone())
        .lease_store(leases)
        .owner_id(instance)
}
// --8<-- [end:owner]

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("dispatch", "0.1.0")).with_broker(broker, |b| {
        b.include(assign);
    })
}

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
    // A pod's name stays with it across restarts and differs between pods.
    let instance = env::var("HOSTNAME").unwrap_or_else(|_| "dispatch-local".to_owned());
    service(broker(&config, "dispatch-leases", &instance))
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
    async fn the_lease_row_names_the_instance() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = support::endpoint() else {
            return Ok(());
        };
        let config = support::config(&endpoint).await;
        let table = support::unique("dispatch-leases");
        support::create_lease_table(&config, &table).await;
        support::ensure_stream(&config, "dispatch", 1).await;

        let tb = TestApp::start_live(service(broker(&config, &table, "dispatch-7"))).await?;
        tb.broker::<KinesisBroker>()
            .message(&Pickup { id: 1 })
            .publish()
            .await?;
        tb.broker::<KinesisBroker>()
            .subscriber("dispatch")
            .assert_called_once();

        assert_eq!(
            support::row_text(
                &config,
                &table,
                "dispatch:shardId-000000000000",
                "lease_owner"
            )
            .await
            .as_deref(),
            Some("dispatch-7"),
        );
        tb.shutdown().await?;
        Ok(())
    }
}
