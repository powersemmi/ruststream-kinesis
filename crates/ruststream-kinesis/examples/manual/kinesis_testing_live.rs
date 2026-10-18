//! The service of `examples/kinesis_testing_live.rs` written without the macros, under the same
//! two tests: one body, in process and against a running stack.
//!
//! ```text
//! cargo test --example manual_kinesis_testing_live --features testing
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example manual_kinesis_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

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
#[outgoing(name = "testing-live-payments")]
struct Payment {
    amount: u64,
}

/// Accepts a payment; a payment of nothing is retired without a retry.
struct Accept;

impl Handle<Payment> for Accept {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.amount == 0 {
            return ready(Err(HandlerOutcome::drop()));
        }
        tracing::info!(payment.amount, "payment accepted");
        ready(Ok(()))
    }
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(KinesisStream::new("testing-live-payments"), Accept).build());
    })
}

fn app() -> impl App {
    service(KinesisBroker::new())
}

fn main() -> ExitCode {
    run_main(app)
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
