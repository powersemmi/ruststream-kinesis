//! The service of `examples/kinesis_subscriptions_shards.rs` written without the macros: one
//! subscription reads every shard of its stream, and a partition key keeps a customer's shipments
//! in order.
//!
//! ```text
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
//!     cargo test --example manual_kinesis_subscriptions_shards --features testing -- --test-threads=1
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Shipment {
    customer: String,
    seq: u64,
}

// `#[outgoing(name = "shipments")]` by hand: the type fixes its destination.
impl OutgoingDestination for Shipment {
    type Form = FixedName;

    const DESTINATION: &'static str = "shipments";
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

// --8<-- [start:shards]
/// Ships each shipment. The stream has two shards, read side by side; a customer's shipments
/// carry the customer as their partition key, so they share a shard and arrive in order.
struct Ship;

impl Handle<Shipment> for Ship {
    fn handle(
        &self,
        shipment: &Shipment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(shipment.customer, shipment.seq, "shipped");
        ready(Ok(()))
    }
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(broker, |b| {
        let shipments = KinesisStream::new("shipments").create_if_missing(2);
        b.include(subscriber(shipments, Ship).build());
    })
}
// --8<-- [end:shards]

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
#[path = "../support/kinesis_live.rs"]
mod support;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::HeaderMap;
    use ruststream::testing::TestApp;
    use ruststream_kinesis::PARTITION_KEY_HEADER;

    use super::*;

    /// Three customers, three shipments each, every shipment keyed by its customer.
    async fn ship_nine(tb: &TestApp<()>) -> Result<(), Box<dyn Error>> {
        for seq in 1..=3 {
            for customer in ["ana", "bo", "cy"] {
                let mut headers = HeaderMap::new();
                headers.insert(PARTITION_KEY_HEADER, customer);
                tb.broker::<KinesisBroker>()
                    .message(&Shipment {
                        customer: customer.to_owned(),
                        seq,
                    })
                    .with_headers(headers)
                    .publish()
                    .await?;
            }
        }
        Ok(())
    }

    /// Each customer's shipments in the order the handler saw them.
    fn per_customer(tb: &TestApp<()>, customer: &str) -> Vec<u64> {
        tb.broker::<KinesisBroker>()
            .subscriber("shipments")
            .received::<Shipment>()
            .into_iter()
            .filter(|shipment| shipment.customer == customer)
            .map(|shipment| shipment.seq)
            .collect()
    }

    async fn each_customer_ships_in_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        ship_nine(&tb).await?;

        tb.broker::<KinesisBroker>()
            .subscriber("shipments")
            .assert_called(9)
            .settled(HandlerOutcome::ack());
        for customer in ["ana", "bo", "cy"] {
            assert_eq!(per_customer(&tb, customer), [1, 2, 3], "{customer}");
        }
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_customer_ships_in_order_in_process() -> Result<(), Box<dyn Error>> {
        each_customer_ships_in_order(TestApp::start(service(KinesisBroker::new())).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_customer_ships_in_order_across_two_shards() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = support::endpoint() else {
            return Ok(());
        };
        let tb = TestApp::start_live(service(support::broker(&endpoint))).await?;
        each_customer_ships_in_order(tb).await
    }
}
