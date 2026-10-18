//! The service of `examples/kinesis_subscriptions_ext.rs` written without the macros: the read
//! settings chain onto `subscriber(..)` after the framework's steps and before `start_at(..)`.
//!
//! ```text
//! cargo test --example manual_kinesis_subscriptions_ext --features testing
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct AuditEvent {
    id: u64,
}

// `#[outgoing(name = "audit")]` by hand: the type fixes its destination.
impl OutgoingDestination for AuditEvent {
    type Form = FixedName;

    const DESTINATION: &'static str = "audit";
}

impl MessageHeaders for AuditEvent {
    type Contract = NoHeaders;
}

// --8<-- [start:chain]
/// Archives every audit event the stream still retains.
struct Archive;

impl Handle<AuditEvent> for Archive {
    fn handle(
        &self,
        event: &AuditEvent,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.id, "archived");
        ready(Ok(()))
    }
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
        // The framework's step first, this crate's settings next, the start position last.
        b.include(
            subscriber(KinesisStream::new("audit"), Archive)
                .workers(nonzero!(4))
                .build()
                .poll_interval(Duration::from_millis(250))
                .create_if_missing(1)
                .start_at(KinesisPosition::horizon()),
        );
    })
}
// --8<-- [end:chain]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let app = service(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
    );
    app.run().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_chained_settings_mount_and_deliver() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        for id in [1, 2] {
            tb.broker::<KinesisBroker>()
                .message(&AuditEvent { id })
                .publish()
                .await?;
        }

        tb.broker::<KinesisBroker>()
            .subscriber("audit")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
