//! The service of the Shards, partition keys and ordering guide, written without the macros: each
//! handler is a type with an `impl Handle`, mounted with the `subscriber` constructor, and `main`
//! is written by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_ordering --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:post]
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
    customer: String,
}

impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "customer-orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

/// One line of a customer's journal.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Entry {
    order: u64,
    customer: String,
}

impl OutgoingDestination for Entry {
    type Form = FixedName;

    const DESTINATION: &'static str = "customer-orders.journal";
}

impl MessageHeaders for Entry {
    type Contract = NoHeaders;
}

impl MessageInfo for Entry {
    const NAME: &'static str = "Entry";
}

struct Journal;

impl OutSlot for Journal {
    const NAME: &'static str = "Journal";
    type Destination = Reads;
}

impl PublishedThrough<Journal> for Entry {}

/// Posts each order to the journal, under the customer as its partition key.
struct Post;

impl<Lines> Handle<Order, (), Outs<(Lines,)>> for Post
where
    Lines: OutEntry<Journal, Wire: Publisher<Options = KinesisPublishOptions>>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Lines,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let entry = Entry {
            order: order.id,
            customer: order.customer.clone(),
        };
        if outs
            .get(Journal)
            .message(&entry)
            .partition_key(order.customer.as_str())
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        tracing::info!(order.id, order.customer, "order posted");
        Ok(())
    }
}
// --8<-- [end:post]

// --8<-- [start:confirm]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Receipt {
    order: u64,
}

impl OutgoingDestination for Receipt {
    type Form = FixedName;

    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

impl MessageInfo for Receipt {
    const NAME: &'static str = "Receipt";
}

/// Confirms each journal entry with a receipt.
struct Confirm;

impl Handle<Entry, Receipt> for Confirm {
    fn handle(
        &self,
        entry: &Entry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { order: entry.order }))
    }
}
// --8<-- [end:confirm]

// --8<-- [start:app]
fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(
                KinesisStream::new("customer-orders").create_if_missing(2),
                Post,
            )
            .workers_by_key(nonzero!(4usize))
            .build(),
        )
        .out(Journal, Publish::default())
        .build();
        // One key for every receipt: one shard, so the receipts keep one order between them.
        b.include(
            subscriber(
                KinesisStream::new("customer-orders.journal").create_if_missing(1),
                Confirm,
            )
            .reply()
            .build(),
        )
        .out_reply(Publish::default())
        .partition_key("receipts");
    })
}

fn main() -> ExitCode {
    run_main(|| {
        service(
            KinesisBroker::new()
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .test_credentials(),
        )
    })
}
// --8<-- [end:app]

#[cfg(test)]
#[path = "../../support/kinesis_live.rs"]
mod support;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::HeaderMap;
    use ruststream::testing::TestApp;
    use ruststream_kinesis::PARTITION_KEY_HEADER;

    use super::*;

    /// Two orders for each of three customers, published one customer after another, each keyed
    /// by its customer.
    async fn place_orders(tb: &TestApp<()>) -> Result<(), Box<dyn Error>> {
        for (id, customer) in [
            (1, "ana"),
            (2, "bo"),
            (3, "cy"),
            (4, "ana"),
            (5, "bo"),
            (6, "cy"),
        ] {
            let mut key = HeaderMap::new();
            key.insert(PARTITION_KEY_HEADER, customer);
            tb.broker::<KinesisBroker>()
                .message(&Order {
                    id,
                    customer: customer.to_owned(),
                })
                .with_headers(key)
                .publish()
                .await?;
        }
        Ok(())
    }

    /// The orders of `customer` in the order the journal delivered them.
    fn orders_of(tb: &TestApp<()>, customer: &str) -> Vec<u64> {
        tb.broker::<KinesisBroker>()
            .subscriber("customer-orders.journal")
            .received::<Entry>()
            .into_iter()
            .filter(|entry| entry.customer == customer)
            .map(|entry| entry.order)
            .collect()
    }

    async fn each_customer_keeps_its_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        place_orders(&tb).await?;

        tb.broker::<KinesisBroker>()
            .subscriber("customer-orders")
            .assert_called(6)
            .settled(HandlerOutcome::ack());
        assert_eq!(orders_of(&tb, "ana"), [1, 4]);
        assert_eq!(orders_of(&tb, "bo"), [2, 5]);
        assert_eq!(orders_of(&tb, "cy"), [3, 6]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn each_customer_keeps_its_order_in_process() -> Result<(), Box<dyn Error>> {
        each_customer_keeps_its_order(TestApp::start(service(KinesisBroker::new())).await?).await
    }

    #[tokio::test]
    async fn a_journal_entry_takes_the_customer_as_its_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        place_orders(&tb).await?;

        tb.out::<Journal>()
            .assert_called(6)
            .with_options(&KinesisPublishOptions {
                partition_key: Some("cy".to_owned()),
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn every_receipt_shares_one_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        place_orders(&tb).await?;

        tb.broker::<KinesisBroker>()
            .published::<Receipt>("receipts")
            .assert_called(6)
            .with_header(PARTITION_KEY_HEADER, "receipts");
        tb.shutdown().await?;
        Ok(())
    }

    /// On a stand the orders stream has two shards, so the customers' orders spread over two
    /// readers, and the lanes still keep each customer in order.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_customer_keeps_its_order_across_two_shards() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = support::endpoint() else {
            return Ok(());
        };
        // Nothing reads the receipts here, so nothing creates their stream.
        support::ensure_stream(&support::config(&endpoint).await, "receipts", 1).await;
        let tb = TestApp::start_live(service(support::broker(&endpoint))).await?;
        each_customer_keeps_its_order(tb).await
    }
}
