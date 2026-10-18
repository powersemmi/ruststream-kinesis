//! The service of `examples/kinesis_broker_shutdown.rs` written without the macros: a publisher
//! taken from the broker publishes while the service runs, and reports
//! `KinesisError::NotConnected` once the service has shut down.
//!
//! ```text
//! cargo test --example manual_kinesis_broker_shutdown --features testing
//! ```

// A plain build compiles the service; the code that cancels orders exists only in the test build.
#![cfg_attr(not(test), allow(dead_code))]

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:service]
use ruststream_kinesis::KinesisPublisher;
use ruststream_kinesis::prelude::*;

/// The service, and a publisher for the code beside it that publishes outside any handler, such
/// as an admin endpoint.
fn service(broker: KinesisBroker) -> (RustStream, KinesisPublisher) {
    let admin = broker.publisher();
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(KinesisStream::new("orders"), Place).build());
    });
    (app, admin)
}
// --8<-- [end:service]

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

fn app() -> impl App {
    let (app, _admin) = service(KinesisBroker::new());
    app
}

fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// What the admin endpoint writes when an operator cancels an order.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "cancellations")]
struct Cancellation {
    order: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::PublishError;
    use ruststream::testing::TestApp;
    use ruststream_kinesis::KinesisError;
    use ruststream_kinesis::prelude::*;

    use super::{Cancellation, service};

    #[tokio::test]
    async fn the_admin_publisher_stops_with_the_service() -> Result<(), Box<dyn Error>> {
        let (app, admin) = service(KinesisBroker::new());
        let tb = TestApp::start(app).await?;

        admin.message(&Cancellation { order: 7 }).publish().await?;
        tb.broker::<KinesisBroker>()
            .published::<Cancellation>("cancellations")
            .assert_called_once()
            .with(&Cancellation { order: 7 });

        tb.shutdown().await?;
        let refused = admin.message(&Cancellation { order: 8 }).publish().await;
        assert!(
            matches!(
                refused,
                Err(PublishError::Publish(KinesisError::NotConnected))
            ),
            "{refused:?}"
        );
        Ok(())
    }
}
