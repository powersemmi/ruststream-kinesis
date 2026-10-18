//! The service of `examples/kinesis_broker_credentials.rs` written without the macros: a broker
//! built from an AWS configuration the service assembles itself.
//!
//! ```text
//! cargo test --example manual_kinesis_broker_credentials --features testing
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example manual_kinesis_broker_credentials --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;

use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_kinesis::config::SharedCredentialsProvider;
use ruststream::runtime::cli::run_main;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../support/kinesis_stand.rs"]
mod stand;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order placed");
        ready(Ok(()))
    }
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(KinesisStream::new("orders"), Place).build());
    })
}

// --8<-- [start:config]
/// The credentials of the `orders` profile in the shared AWS files, in `eu-west-1`. The broker
/// uses the config as it is.
fn config() -> SdkConfig {
    let profile = ProfileFileCredentialsProvider::builder()
        .profile_name("orders")
        .build();
    SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("eu-west-1"))
        .credentials_provider(SharedCredentialsProvider::new(profile))
        .build()
}

fn app() -> impl App {
    service(KinesisBroker::from_config(config()))
}
// --8<-- [end:config]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use aws_config::{BehaviorVersion, Region, SdkConfig};
    use aws_sdk_kinesis::config::{Credentials, SharedCredentialsProvider};
    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Order, config, service, stand};

    /// The body both modes run: an order published to the stream is placed.
    async fn places_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<KinesisBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        places_an_order(TestApp::start(service(KinesisBroker::from_config(config()))).await?).await
    }

    /// The same kind of config, pointed at the stand with the static key a stand accepts. The
    /// endpoint rides the config: the broker's own `endpoint` step does not apply to it.
    fn stand_config(endpoint: &str) -> SdkConfig {
        let key = Credentials::new("test", "test", None, None, "stand");
        SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .region(Region::new("us-east-1"))
            .credentials_provider(SharedCredentialsProvider::new(key))
            .build()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = stand::endpoint() else {
            return Ok(());
        };
        stand::ensure_stream(&stand::config(&endpoint).await, "orders").await;
        let broker = KinesisBroker::from_config(stand_config(&endpoint));
        places_an_order(TestApp::start_live_within(service(broker), stand::START).await?).await
    }
}
