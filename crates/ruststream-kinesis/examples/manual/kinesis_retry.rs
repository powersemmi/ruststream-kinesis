//! The service of `examples/kinesis_retry.rs` written without the macros: a delayed retry on the
//! error side, and the cap and dead-letter stream at the mount site.
//!
//! ```text
//! cargo test --example manual_kinesis_retry --features testing
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

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

/// How long an order that cannot be settled yet waits for its next delivery.
const PAUSE: Duration = Duration::from_secs(30);

// --8<-- [start:handler]
/// Asks for a pause when the order cannot be settled yet.
struct Reconcile;

impl Handle<Order> for Reconcile {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(if settled(order.id) {
            Ok(())
        } else {
            Err(HandlerOutcome::retry_after(PAUSE))
        })
    }
}

fn settled(id: u64) -> bool {
    id.is_multiple_of(2)
}
// --8<-- [end:handler]

// --8<-- [start:retry]
fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // Four deliveries per order, then the record leaves for a stream an operator reads.
        b.include(subscriber(KinesisStream::new("orders"), Reconcile).build())
            .max_attempts(nonzero!(4))
            .dead_letter("orders.unsettled");
    })
}
// --8<-- [end:retry]

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

    use ruststream::HeaderMap;
    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_kinesis::PARTITION_KEY_HEADER;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn an_unsettled_order_comes_back_after_the_pause() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "customer-7");

        tb.broker::<KinesisBroker>()
            .message(&Order { id: 1 })
            .with_headers(headers)
            .publish()
            .await?;
        tb.broker::<KinesisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(PAUSE));

        tb.advance(PAUSE).await?;

        tb.broker::<KinesisBroker>()
            .subscriber("orders")
            .assert_called(2);
        // The copy counts the delivery and keeps the key, so it lands on the original's shard.
        tb.broker::<KinesisBroker>()
            .published::<Order>("orders")
            .assert_called(2)
            .with(&Order { id: 1 })
            .with_header(RETRY_COUNT_HEADER, "1")
            .with_header(PARTITION_KEY_HEADER, "customer-7");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_spent_order_leaves_for_the_dead_letter_stream() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        tb.broker::<KinesisBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;
        for _ in 0..3 {
            tb.advance(PAUSE).await?;
        }

        tb.broker::<KinesisBroker>()
            .subscriber("orders")
            .assert_called(4);
        tb.broker::<KinesisBroker>()
            .published::<Order>("orders.unsettled")
            .assert_called_once()
            .with(&Order { id: 1 })
            .with_header(RETRY_COUNT_HEADER, "4");
        tb.shutdown().await?;
        Ok(())
    }
}
