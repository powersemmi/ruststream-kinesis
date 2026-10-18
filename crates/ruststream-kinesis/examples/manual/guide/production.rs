//! The service of the Running in production guide, written without the macros: the broker is built from an AWS configuration
//! of the service's own, with a pinned behaviour version, a region and the credentials of a named
//! profile, and it reads a stream its infrastructure owns.
//!
//! ```text
//! cargo run --example manual_guide_production --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_kinesis::config::SharedCredentialsProvider;
use ruststream::runtime::cli::run_main;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:config]
/// The AWS configuration of the service: a pinned behaviour version, its region, and the
/// credentials of the `orders-service` profile, read when the first request is signed.
fn aws_config() -> SdkConfig {
    let credentials = ProfileFileCredentialsProvider::builder()
        .profile_name("orders-service")
        .build();
    SdkConfig::builder()
        .behavior_version(BehaviorVersion::v2026_01_12())
        .region(Region::new("eu-west-1"))
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        .build()
}
// --8<-- [end:config]

// --8<-- [start:service]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
struct Order {
    id: u64,
    amount_cents: u64,
}

impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

struct TakeOrder;

impl Handle<Order> for TakeOrder {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if order.amount_cents == 0 {
            tracing::warn!(order.id, "order without an amount dropped");
            return ready(Err(HandlerOutcome::drop()));
        }
        tracing::info!(order.id, order.amount_cents, "order taken");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisBroker::from_config(aws_config()),
        |b| {
            // The stream belongs to the infrastructure, so the descriptor does not create it.
            b.include(subscriber(KinesisStream::new("orders"), TakeOrder).build());
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn the_configured_broker_takes_an_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<KinesisBroker>()
            .message(&Order {
                id: 1,
                amount_cents: 1_250,
            })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_order_without_an_amount_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<KinesisBroker>()
            .message(&Order {
                id: 2,
                amount_cents: 0,
            })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
