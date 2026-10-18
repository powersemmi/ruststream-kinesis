//! A subscription described by `KinesisStream`: the stream name, the pause between reads of an
//! idle shard, and the shards to provision when the stream is missing. Each scenario is a test,
//! and `cargo run` starts the service against a local stack.
//!
//! ```text
//! cargo test --example kinesis_subscriptions_stream --features testing
//! ```

use std::time::Duration;

use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
}

// --8<-- [start:descriptor]
/// Captures each payment. The descriptor names the stream and the settings of its reads.
#[subscriber(
    KinesisStream::new("payments")
        .poll_interval(Duration::from_millis(500))
        .create_if_missing(4)
)]
async fn capture(payment: &Payment) -> HandlerOutcome {
    tracing::info!(payment.id, "payment captured");
    HandlerOutcome::ack()
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(capture);
    })
}
// --8<-- [end:descriptor]

#[ruststream::app]
fn app() -> impl App {
    service(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn the_subscription_is_named_after_its_stream() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        tb.broker::<KinesisBroker>()
            .message(&Payment { id: 3 })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&Payment { id: 3 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
