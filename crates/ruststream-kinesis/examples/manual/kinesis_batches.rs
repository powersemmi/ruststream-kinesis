//! The service of `examples/kinesis_batches.rs` written without the macros: a body over `[T]` is a
//! batch handler, and its error side is one outcome per record.
//!
//! ```text
//! cargo test --example manual_kinesis_batches --features testing
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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Refund {
    id: u64,
    approved: bool,
}

// `#[outgoing(name = "refunds")]` by hand: the type fixes its destination.
impl OutgoingDestination for Refund {
    type Form = FixedName;

    const DESTINATION: &'static str = "refunds";
}

impl MessageHeaders for Refund {
    type Contract = NoHeaders;
}

/// How long an unapproved refund waits before it is checked again.
const RECHECK: Duration = Duration::from_secs(30);

// --8<-- [start:batches]
/// Stores a batch of orders in one round-trip and settles the whole batch with one outcome.
struct Digest;

impl Handle<[Order]> for Digest {
    fn handle(
        &self,
        batch: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = batch.len(), "orders stored");
        ready(Ok(()))
    }
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // The framework's step first, this crate's read setting after it.
        b.include(
            subscriber(KinesisStream::new("orders"), Digest)
                .batch(nonzero!(500))
                .build()
                .poll_interval(Duration::from_millis(500)),
        );
        b.include(
            subscriber(KinesisStream::new("refunds"), Refunds)
                .batch(nonzero!(100))
                .build(),
        );
    })
}
// --8<-- [end:batches]

// --8<-- [start:per_record]
/// One outcome per refund, in slice order: only the unapproved ones come back.
struct Refunds;

impl Handle<[Refund]> for Refunds {
    fn handle(
        &self,
        batch: &[Refund],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        ready(Err(batch
            .iter()
            .map(|refund| {
                if refund.approved {
                    HandlerOutcome::ack()
                } else {
                    HandlerOutcome::retry_after(RECHECK)
                }
            })
            .collect()))
    }
}
// --8<-- [end:per_record]

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

    use futures::future::try_join_all;
    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn orders_published_together_arrive_in_one_batch() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        let orders: Vec<Order> = (1..=3).map(|id| Order { id }).collect();
        let broker = tb.broker::<KinesisBroker>();
        try_join_all(orders.iter().map(|order| broker.message(order).publish())).await?;

        tb.broker::<KinesisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn only_the_unapproved_refund_comes_back() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        let refunds: Vec<Refund> = [(1, true), (2, false), (3, true)]
            .into_iter()
            .map(|(id, approved)| Refund { id, approved })
            .collect();
        let broker = tb.broker::<KinesisBroker>();
        try_join_all(
            refunds
                .iter()
                .map(|refund| broker.message(refund).publish()),
        )
        .await?;
        tb.advance(RECHECK).await?;

        let ids: Vec<Vec<u64>> = tb
            .broker::<KinesisBroker>()
            .subscriber("refunds")
            .batches::<Refund>()
            .iter()
            .map(|batch| batch.iter().map(|refund| refund.id).collect())
            .collect();
        assert_eq!(ids, [vec![1, 2, 3], vec![2]]);
        tb.shutdown().await?;
        Ok(())
    }
}
