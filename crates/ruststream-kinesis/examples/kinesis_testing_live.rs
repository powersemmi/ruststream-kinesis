//! One test body, two modes: in process under `TestApp::start`, and against a running stack under
//! `TestApp::start_live`. The live test skips without `KINESIS_TEST_ENDPOINT`, and
//! `RUSTSTREAM_REQUIRE_LIVE=1` turns that skip into a failure.
//!
//! ```text
//! cargo test --example kinesis_testing_live --features testing
//! just brokers-up
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example kinesis_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

#[cfg(test)]
#[path = "support/kinesis_stand.rs"]
mod stand;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "testing-live-payments")]
struct Payment {
    amount: u64,
}

#[subscriber(KinesisStream::new("testing-live-payments"))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    tracing::info!(payment.amount, "payment accepted");
    HandlerOutcome::ack()
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(accept);
    })
}

#[ruststream::app]
fn app() -> impl App {
    service(KinesisBroker::new())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Payment, service, stand};

    // --8<-- [start:start-live]
    /// The body both modes run; only the start call differs.
    async fn accepts_a_payment(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<KinesisBroker>()
            .message(&Payment { amount: 100 })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("testing-live-payments")
            .assert_called_once()
            .with(&Payment { amount: 100 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        accepts_a_payment(TestApp::start(service(KinesisBroker::new())).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = stand::endpoint() else {
            return Ok(());
        };
        // A subscription opens at the tip, past what earlier runs left in the stream.
        stand::ensure_stream(&stand::config(&endpoint).await, "testing-live-payments").await;
        let tb =
            TestApp::start_live_within(service(stand::broker(&endpoint)), stand::START).await?;
        accepts_a_payment(tb).await
    }
    // --8<-- [end:start-live]
}
