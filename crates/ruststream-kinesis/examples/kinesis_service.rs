//! A minimal Kinesis service: consume a stream with per-shard checkpointing.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example kinesis_service`

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_kinesis::{KinesisBroker, KinesisStream, StartPosition};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(KinesisStream::new("orders").start(StartPosition::Horizon))]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
        |b| {
            b.include(handle);
        },
    )
}
