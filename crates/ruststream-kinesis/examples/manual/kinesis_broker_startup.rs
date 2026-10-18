//! The service of `examples/kinesis_broker_startup.rs` written without the macros: a
//! subscription opens once every shard it took has a reader, and a stream the service does not
//! have stops the service before it reads anything.
//!
//! ```text
//! cargo test --example manual_kinesis_broker_startup --features testing
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example manual_kinesis_broker_startup --features testing
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
#[outgoing(name = "payouts")]
struct Payout {
    order: u64,
    cents: u64,
}

// --8<-- [start:service]
/// Pays out a settled order. The stream `payouts` is the platform team's: the service reads it
/// and never creates it.
struct Pay;

impl Handle<Payout> for Pay {
    fn handle(
        &self,
        payout: &Payout,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(payout.order, payout.cents, "payout sent");
        ready(Ok(()))
    }
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("payouts", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(KinesisStream::new("payouts"), Pay).build());
    })
}
// --8<-- [end:service]

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
    use ruststream_kinesis::KinesisError;
    use ruststream_kinesis::prelude::*;

    use super::{Payout, app, service, stand};

    #[tokio::test]
    async fn a_payout_published_right_after_startup_is_read() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Payout {
                order: 7,
                cents: 1250,
            })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("payouts")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    /// The stand never has `payouts`, so the service stops at startup with the stream's error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_missing_stream_stops_the_service_at_startup() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = stand::endpoint() else {
            return Ok(());
        };
        let Err(failed) =
            TestApp::start_live_within(service(stand::broker(&endpoint)), stand::START).await
        else {
            panic!("a subscription on a stream the stand lacks must not open");
        };

        let mut cause: Option<&(dyn Error + 'static)> = Some(&failed);
        while let Some(err) = cause {
            if let Some(kinesis) = err.downcast_ref::<KinesisError>() {
                assert!(
                    matches!(kinesis, KinesisError::Stream { stream, .. } if stream == "payouts"),
                    "{kinesis}"
                );
                return Ok(());
            }
            cause = err.source();
        }
        panic!("startup did not fail with a KinesisError: {failed}");
    }
}
