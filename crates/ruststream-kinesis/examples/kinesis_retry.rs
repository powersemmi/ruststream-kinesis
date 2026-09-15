//! Retrying a record after a delay, which Kinesis itself cannot hold back.
//!
//! The framework re-publishes the record once the delay is over, and stops once the registration
//! says the attempts are spent. Run a local stack first (`just brokers-up`), then:
//! `cargo run --example kinesis_retry`

// --8<-- [start:handler]
use ruststream_kinesis::prelude::*;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// Asks for a pause when the order cannot be settled yet. How many pauses it gets is the mount
/// site's to say, so the handler never counts them.
#[subscriber(KinesisStream::new("orders"))]
async fn reconcile(order: &Order) -> HandlerOutcome {
    if settled(order.id) {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry_after(Duration::from_secs(30))
    }
}

fn settled(id: u64) -> bool {
    id.is_multiple_of(2)
}
// --8<-- [end:handler]

// --8<-- [start:retry]
#[ruststream::app]
fn app() -> impl App {
    let broker = KinesisBroker::new()
        .endpoint("http://localhost:4566")
        .test_credentials()
        .region("us-east-1");

    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // Four deliveries per order, then the record leaves for a stream an operator reads.
        b.include(reconcile)
            .max_attempts(nonzero!(4))
            .dead_letter("orders.unsettled");
    })
}
// --8<-- [end:retry]
