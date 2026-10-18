//! A timestamp position: the report opens every shard at the first reading that arrived at or
//! after the instant its configuration names. The in-process test runs everywhere; the live one
//! shows the readings that arrived before the service started, against the stand at
//! `KINESIS_TEST_ENDPOINT`, and skips without it.
//!
//! ```text
//! cargo test --example kinesis_positions_timestamp --features testing
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example kinesis_positions_timestamp --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;

use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

#[cfg(test)]
#[path = "support/kinesis_stand.rs"]
mod stand;

// --8<-- [start:replay]
/// Adds a reading to the report.
#[subscriber(KinesisStream::new("readings"))]
async fn report(reading: &Reading) -> HandlerOutcome {
    tracing::info!(reading.sensor, reading.value, "reading reported");
    HandlerOutcome::ack()
}

/// The report opens every shard at the first reading that arrived at or after `since`, in
/// milliseconds since the Unix epoch.
fn service(broker: KinesisBroker, since: u64) -> RustStream {
    let _ = since;
    RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(broker, |b| {
        b.include(report);
    })
}
// --8<-- [end:replay]

#[ruststream::app]
fn app() -> impl App {
    let since = env::var("REPORT_SINCE_MILLIS")
        .ok()
        .and_then(|millis| millis.parse().ok())
        .unwrap_or(0);
    service(KinesisBroker::new(), since)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "readings")]
struct Reading {
    sensor: String,
    value: u64,
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

        let tb =
            TestApp::start_live_within(service(stand::broker(&endpoint), since), stand::START)
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
