//! A minimal Kinesis service: consume a stream with per-shard checkpointing.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example kinesis_service`

// --8<-- [start:handler]
use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_kinesis::{KinesisBroker, KinesisPosition, KinesisStream};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// Without the `start_at` clause each shard would resume from its checkpoint and otherwise
// open at the tip; the horizon replays everything the stream still retains.
#[subscriber(KinesisStream::new("orders"), start_at(KinesisPosition::horizon()))]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:handler]

// --8<-- [start:app]
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
// --8<-- [end:app]
