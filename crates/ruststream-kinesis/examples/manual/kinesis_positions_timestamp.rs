//! The service of `examples/kinesis_positions_timestamp.rs` written without the macros:
//! `.start_at(position)` is a step of the mount chain.
//!
//! ```text
//! cargo test --example manual_kinesis_positions_timestamp --features testing
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example manual_kinesis_positions_timestamp --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../support/kinesis_stand.rs"]
mod stand;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "readings")]
struct Reading {
    sensor: String,
    value: u64,
}

// --8<-- [start:replay]
/// Adds a reading to the report.
struct Report;

impl Handle<Reading> for Report {
    fn handle(
        &self,
        reading: &Reading,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(reading.sensor, reading.value, "reading reported");
        ready(Ok(()))
    }
}

/// The report opens every shard at the first reading that arrived at or after `since`, in
/// milliseconds since the Unix epoch.
fn service(broker: KinesisBroker, since: u64) -> RustStream {
    RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(KinesisStream::new("readings"), Report)
                .build()
                .start_at(KinesisPosition::timestamp(since)),
        );
    })
}
// --8<-- [end:replay]

fn app() -> impl App {
    let since = env::var("REPORT_SINCE_MILLIS")
        .ok()
        .and_then(|millis| millis.parse().ok())
        .unwrap_or(0);
    service(KinesisBroker::new(), since)
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Reading, service, stand};

    fn reading(value: u64) -> Reading {
        Reading {
            sensor: "boiler-3".to_owned(),
            value,
        }
    }

    #[tokio::test]
    async fn a_reading_after_the_instant_is_reported() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new(), 0)).await?;
        tb.broker::<KinesisBroker>()
            .message(&reading(71))
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("readings")
            .assert_called_once()
            .with(&reading(71))
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    /// A reading that arrived before the service started, after the instant, is reported first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reading_from_before_startup_is_reported() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = stand::endpoint() else {
            return Ok(());
        };
        let config = stand::config(&endpoint).await;
        stand::recreate_stream(&config, "readings").await;
        // The service stamps arrival approximately, so the instant sits a second before the write.
        let since = stand::now_millis() - 1000;
        stand::put(&config, "readings", &serde_json::to_vec(&reading(70))?).await;

        let tb = TestApp::start_live_within(service(stand::broker(&endpoint), since), stand::START)
            .await?;
        tb.broker::<KinesisBroker>()
            .message(&reading(71))
            .publish()
            .await?;

        // A live publish returns once the subscription has handled as many records as the test
        // published, so the first of them is what decides.
        let reported = tb
            .broker::<KinesisBroker>()
            .subscriber("readings")
            .received::<Reading>();
        assert_eq!(reported.first(), Some(&reading(70)));
        tb.shutdown().await?;
        Ok(())
    }
}
