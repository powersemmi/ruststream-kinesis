//! `endpoint`, `region` and `test_credentials` point the broker at a local stack. The
//! in-process test runs everywhere; the live one runs against the stand at
//! `KINESIS_TEST_ENDPOINT` and skips without it.
//!
//! ```text
//! cargo test --example kinesis_broker_endpoint --features testing
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example kinesis_broker_endpoint --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

#[cfg(test)]
#[path = "support/kinesis_stand.rs"]
mod stand;

#[subscriber(KinesisStream::new("telemetry"))]
async fn record(reading: &Reading) -> HandlerOutcome {
    tracing::info!(reading.sensor, reading.value, "reading recorded");
    HandlerOutcome::ack()
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(broker, |b| {
        b.include(record);
    })
}

// --8<-- [start:broker]
/// The broker on the local stack at `endpoint`: the region is named, and the credentials are
/// dummies the stack accepts.
fn broker(endpoint: &str) -> KinesisBroker {
    KinesisBroker::new()
        .endpoint(endpoint)
        .region("us-east-1")
        .test_credentials()
}

#[ruststream::app]
fn app() -> impl App {
    service(broker("http://localhost:4566"))
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "telemetry")]
struct Reading {
    sensor: String,
    value: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Reading, broker, service, stand};

    /// The body both modes run: a reading published to the stream is recorded.
    async fn records_a_reading(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        let reading = Reading {
            sensor: "boiler-3".to_owned(),
            value: 71,
        };
        tb.broker::<KinesisBroker>()
            .message(&reading)
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("telemetry")
            .assert_called_once()
            .with(&reading)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        records_a_reading(TestApp::start(service(broker("http://localhost:4566"))).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = stand::endpoint() else {
            return Ok(());
        };
        stand::ensure_stream(&stand::config(&endpoint).await, "telemetry").await;
        let tb = TestApp::start_live_within(service(broker(&endpoint)), stand::START).await?;
        records_a_reading(tb).await
    }
}
