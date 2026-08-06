//! Sharing the shards between service instances through a `DynamoDB` lease store.
//!
//! Every instance runs the same binary against the same lease table. Leases are raced with
//! conditional writes, so one instance reads a shard at a time, and the checkpoints survive
//! a restart. Requires the `dynamodb-lease` feature.
//!
//! The AWS config is resolved here rather than by the broker, because the lease store needs
//! it too, so this example drives the runtime directly instead of through
//! `#[ruststream::app]`.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example kinesis_leases --features dynamodb-lease`

use std::error::Error;
use std::sync::Arc;

use aws_config::{BehaviorVersion, Region};
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_kinesis::{DynamoLeaseStore, KinesisBroker, KinesisStream};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// No `start_at` clause: each shard resumes from its stored checkpoint, which is what the
// shared lease table is for.
#[subscriber(KinesisStream::new("orders"))]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url("http://localhost:4566")
        .test_credentials()
        .region(Region::new("us-east-1"))
        .load()
        .await;

    // --8<-- [start:leases]
    let leases = Arc::new(DynamoLeaseStore::new(&config, "orders-leases"));
    let broker = KinesisBroker::from_config(config)
        .lease_store(leases)
        // Identifies this instance in the lease table; a process-unique value is used when
        // left out.
        .owner_id("instance-a");

    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(handle);
    });
    // --8<-- [end:leases]

    app.run().await?;
    Ok(())
}
