//! Retrying a record after a delay, which Kinesis itself cannot hold back: the framework
//! publishes a copy once the delay is over, and stops once the registration says the attempts are
//! spent. Each scenario is a test, and `cargo run` starts the service against a local stack.
//!
//! ```text
//! cargo test --example kinesis_retry --features testing
//! ```

use std::time::Duration;

use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// How long an order that cannot be settled yet waits for its next delivery.
const PAUSE: Duration = Duration::from_secs(30);

// --8<-- [start:handler]
/// Asks for a pause when the order cannot be settled yet.
#[subscriber(KinesisStream::new("orders"))]
async fn reconcile(order: &Order) -> HandlerOutcome {
    if settled(order.id) {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry_after(PAUSE)
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
        b.include(reconcile)
            .max_attempts(nonzero!(4))
            .dead_letter("orders.unsettled");
    })
}
// --8<-- [end:retry]

#[ruststream::app]
fn app() -> impl App {
    service(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
    )
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
