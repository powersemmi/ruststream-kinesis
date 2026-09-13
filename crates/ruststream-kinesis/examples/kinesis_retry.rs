//! Retrying a record after a delay, which Kinesis itself cannot hold back.
//!
//! The framework re-publishes the record once the delay is over, through the publisher the mount
//! site names. Run a local stack first (`just brokers-up`), then:
//! `cargo run --example kinesis_retry`

// --8<-- [start:handler]
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream_kinesis::prelude::*;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// Asks for a pause when the order cannot be settled yet, and gives up after three attempts.
/// The count rides the header the framework increments on each re-publish.
#[subscriber(KinesisStream::new("orders"))]
async fn reconcile(order: &Order, ctx: &mut Context<'_, KinesisContext>) -> HandlerOutcome {
    let attempt = ctx
        .headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if settled(order.id) {
        HandlerOutcome::ack()
    } else if attempt < 3 {
        HandlerOutcome::retry_after(Duration::from_secs(30))
    } else {
        // Out of attempts: checkpoint past it so the shard's watermark is not held by one order.
        HandlerOutcome::drop()
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
        // The deferred copy is an ordinary publish, so the registration names the policy it
        // leaves through. Nothing else is needed: the descriptor answers where the copy goes,
        // and a stream is both what the subscription reads and what a publish reaches.
        b.include(reconcile).out_retry(Publish::default());
    })
}
// --8<-- [end:retry]
