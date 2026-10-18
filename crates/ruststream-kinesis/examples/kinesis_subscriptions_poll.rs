//! `poll_interval`: the pause between two reads of an idle shard. Each scenario is a test, and
//! `cargo run` starts the service against a local stack.
//!
//! ```text
//! cargo test --example kinesis_subscriptions_poll --features testing
//! ```

use std::time::Duration;

use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "quotes")]
struct Quote {
    symbol: String,
    price: u64,
}

// --8<-- [start:poll]
/// Reprices on every quote. A quiet shard is read again 200 ms after an empty read, so a new
/// quote waits at most that long; every read counts against the shard's five a second.
#[subscriber(KinesisStream::new("quotes").poll_interval(Duration::from_millis(200)))]
async fn reprice(quote: &Quote) -> HandlerOutcome {
    tracing::info!(quote.symbol, quote.price, "repriced");
    HandlerOutcome::ack()
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("pricing", "0.1.0")).with_broker(broker, |b| {
        b.include(reprice);
    })
}
// --8<-- [end:poll]

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
    async fn a_quote_reaches_the_handler() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;
        let quote = Quote {
            symbol: "ACME".to_owned(),
            price: 1250,
        };

        tb.broker::<KinesisBroker>()
            .message(&quote)
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("quotes")
            .assert_called_once()
            .with(&quote)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
