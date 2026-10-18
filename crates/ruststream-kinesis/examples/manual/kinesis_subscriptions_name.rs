//! The service of `examples/kinesis_subscriptions_name.rs` written without the macros: a stream
//! name in `subscriber(..)` reads every shard of that stream.
//!
//! ```text
//! cargo test --example manual_kinesis_subscriptions_name --features testing
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:name]
/// Reads every shard of the stream `orders`.
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
        b.include(subscriber("orders", Place).build());
    })
}
// --8<-- [end:name]

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
    async fn a_record_on_the_stream_reaches_the_handler() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

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
}
