//! Consuming a Kinesis stream in pages.
//!
//! A page handler takes a slice, and its mount site names the one number the framework carries
//! down to a broker: the page size. On this broker it becomes the `GetRecords` limit every
//! shard reader asks with, so a read never fetches more than one page's worth and a page never
//! carries more records than it named. Everything that prices a read stays this crate's own
//! vocabulary and chains after it.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example kinesis_pages`

// --8<-- [start:pages]
use std::time::Duration;

use ruststream_kinesis::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// Settles the whole page with one outcome: on this broker an acknowledgement is a per-shard
/// checkpoint, so the records ahead of an unhandled one keep the watermark where it is.
#[subscriber(KinesisStream::new("orders"))]
async fn digest(page: &[Order]) -> HandlerOutcome {
    println!("got a page of {}", page.len());
    for order in page {
        println!("  order {}", order.id);
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
        |b| {
            // The framework's word first, this crate's after it.
            b.include(
                digest
                    .batch(nonzero!(500))
                    .poll_interval(Duration::from_millis(500)),
            );
        },
    )
}
// --8<-- [end:pages]
