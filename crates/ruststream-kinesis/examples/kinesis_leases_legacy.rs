//! `legacy_stream`: a lease table written by an earlier version of this crate, whose rows name a
//! shard without its stream, resumed for the one stream it served. The scenario runs against a
//! stand with `DynamoDB`.
//!
//! ```text
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
//!     cargo test --example kinesis_leases_legacy --features testing,dynamodb-lease -- --test-threads=1
//! ```

use std::sync::Arc;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_kinesis::config::{Credentials, SharedCredentialsProvider};
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "returns")]
struct Return {
    id: u64,
}

#[subscriber(KinesisStream::new("returns"))]
async fn refund(item: &Return) -> HandlerOutcome {
    tracing::info!(item.id, "refunded");
    HandlerOutcome::ack()
}

// --8<-- [start:legacy]
/// The table was written by an earlier version for the stream `returns` alone, so its bare rows
/// are that stream's progress.
fn broker(config: &SdkConfig, table: &str) -> KinesisBroker {
    let leases = Arc::new(DynamoLeaseStore::new(config, table).legacy_stream("returns"));
    KinesisBroker::from_config(config.clone()).lease_store(leases)
}
// --8<-- [end:legacy]

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("returns", "0.1.0")).with_broker(broker, |b| {
        b.include(refund);
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
    service(broker(&config, "returns-leases"))
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
    async fn the_stream_resumes_from_its_bare_row() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = support::endpoint() else {
            return Ok(());
        };
        let config = support::config(&endpoint).await;
        let table = support::unique("returns-leases");
        support::create_lease_table(&config, &table).await;
        support::ensure_stream(&config, "returns", 1).await;

        // What the earlier version left behind: return 1 handled, its checkpoint in a bare row.
        let handled = support::put_raw(&config, "returns", r#"{"id":1}"#).await;
        support::put_raw(&config, "returns", r#"{"id":2}"#).await;
        support::put_bare_row(&config, &table, "shardId-000000000000", &handled).await;

        let tb = TestApp::start_live(service(broker(&config, &table))).await?;
        tb.broker::<KinesisBroker>()
            .message(&Return { id: 3 })
            .publish()
            .await?;

        let read = tb
            .broker::<KinesisBroker>()
            .subscriber("returns")
            .received::<Return>();
        assert_eq!(read[0], Return { id: 2 });
        // The first read copied the checkpoint under the row the current version keeps.
        assert!(
            support::row_text(
                &config,
                &table,
                "returns:shardId-000000000000",
                "checkpoint"
            )
            .await
            .is_some()
        );
        tb.shutdown().await?;
        Ok(())
    }
}
