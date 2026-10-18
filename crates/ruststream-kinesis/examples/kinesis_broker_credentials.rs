//! Credentials: a broker built from an AWS configuration the service assembles itself, with
//! `from_config`, instead of the default chain the broker resolves at startup.
//!
//! ```text
//! cargo test --example kinesis_broker_credentials --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:config]
use std::env;

use aws_config::SdkConfig;
use aws_sdk_kinesis::config::{BehaviorVersion, Region};
use ruststream_kinesis::prelude::*;

/// The AWS configuration of the service: a fixed region and the keys of its IAM user, which the
/// deployment hands over under variables of the service's own.
fn aws() -> SdkConfig {
    let _ = env::var_os("ORDERS_AWS_ACCESS_KEY_ID");
    SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("eu-west-1"))
        .build()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisBroker::from_config(aws()),
        |b| {
            let _ = b;
        },
    )
}
// --8<-- [end:config]

#[subscriber(KinesisStream::new("orders"))]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn a_service_on_its_own_configuration_places_orders() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
