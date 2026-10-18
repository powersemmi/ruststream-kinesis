//! The service of `examples/kinesis_subscriptions_stream.rs` written without the macros: a
//! `KinesisStream` in `subscriber(..)` names the stream and the settings of its reads.
//!
//! ```text
//! cargo test --example manual_kinesis_subscriptions_stream --features testing
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Payment {
    id: u64,
}

// `#[outgoing(name = "payments")]` by hand: the type fixes its destination.
impl OutgoingDestination for Payment {
    type Form = FixedName;

    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

// --8<-- [start:descriptor]
/// Captures each payment. The descriptor names the stream and the settings of its reads.
struct Capture;

impl Handle<Payment> for Capture {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(payment.id, "payment captured");
        ready(Ok(()))
    }
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        let payments = KinesisStream::new("payments")
            .poll_interval(Duration::from_millis(500))
            .create_if_missing(4);
        b.include(subscriber(payments, Capture).build());
    })
}
// --8<-- [end:descriptor]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let app = service(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
    );
    app.run().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn the_subscription_is_named_after_its_stream() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        tb.broker::<KinesisBroker>()
            .message(&Payment { id: 3 })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&Payment { id: 3 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
