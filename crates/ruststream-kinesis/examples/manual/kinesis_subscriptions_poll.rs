//! The service of `examples/kinesis_subscriptions_poll.rs` written without the macros:
//! `poll_interval` on the descriptor sets the pause between reads of an idle shard.
//!
//! ```text
//! cargo test --example manual_kinesis_subscriptions_poll --features testing
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Quote {
    symbol: String,
    price: u64,
}

// `#[outgoing(name = "quotes")]` by hand: the type fixes its destination.
impl OutgoingDestination for Quote {
    type Form = FixedName;

    const DESTINATION: &'static str = "quotes";
}

impl MessageHeaders for Quote {
    type Contract = NoHeaders;
}

// --8<-- [start:poll]
/// Reprices on every quote. A quiet shard is read again 200 ms after an empty read, so a new
/// quote waits at most that long; every read counts against the shard's five a second.
struct Reprice;

impl Handle<Quote> for Reprice {
    fn handle(
        &self,
        quote: &Quote,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(quote.symbol, quote.price, "repriced");
        ready(Ok(()))
    }
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("pricing", "0.1.0")).with_broker(broker, |b| {
        let quotes = KinesisStream::new("quotes").poll_interval(Duration::from_millis(200));
        b.include(subscriber(quotes, Reprice).build());
    })
}
// --8<-- [end:poll]

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
