//! Replying on a second stream, with the reply's partition key named at the mount site.
//!
//! A `publish(..)` handler's reply leaves through a publisher the service never holds: the
//! runtime pairs it from the policy the mount site names. `.out(Reply, ..)` is where that policy
//! is named, and this crate's publish settings chain after it - so the shard a receipt lands on,
//! and with it the order receipts keep, is a decision of the mount rather than of the handler.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example kinesis_replies`

// --8<-- [start:replies]
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize)]
struct Receipt {
    order: u64,
}

/// The handler names what it replies with; where that reply goes, and how, is the mount's.
#[subscriber(KinesisStream::new("orders"), publish("receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
        |b| {
            // One key means one shard: every receipt is ordered against every other, at the cost
            // of a single shard's throughput. Leave it off to spread them instead.
            b.include(confirm)
                .out(Reply, Publish::default())
                .partition_key("receipts-v1");
        },
    )
}
// --8<-- [end:replies]
