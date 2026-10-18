//! The service of `examples/kinesis_settlement_out_retry.rs` written without the macros:
//! `out_retry` replaces the publisher the retry copies leave through, and a transform stamps each
//! copy.
//!
//! ```text
//! cargo test --example manual_kinesis_settlement_out_retry --features testing
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::runtime::{
    ForReply, Outgoing, PublishContext, PublishTransform, RETRY_COUNT_HEADER, Reads,
};
use ruststream::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Invoice {
    id: u64,
}

// `#[outgoing(name = "invoices")]` by hand: the type fixes its destination.
impl OutgoingDestination for Invoice {
    type Form = FixedName;

    const DESTINATION: &'static str = "invoices";
}

impl MessageHeaders for Invoice {
    type Contract = NoHeaders;
}

/// The header every retry copy carries: the stream the record was deferred from.
const DEFERRED_FROM: &str = "x-deferred-from";

/// How long an invoice the bank has not confirmed waits for its next delivery.
const PAUSE: Duration = Duration::from_secs(60);

/// Waits for the bank's confirmation, which the first delivery never has.
struct Collect;

impl Handle<Invoice> for Collect {
    fn handle(
        &self,
        _invoice: &Invoice,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
            Err(HandlerOutcome::retry_after(PAUSE))
        } else {
            Ok(())
        })
    }
}

// --8<-- [start:out_retry]
/// Writes the stream a record was deferred from onto its retry copy.
struct DeferredFrom;

impl<Context, Options> PublishTransform<ForReply<Context>, Options> for DeferredFrom {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, Context>,
    ) {
        out.headers_mut()
            .insert(DEFERRED_FROM, cx.name().to_owned());
    }
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(KinesisStream::new("invoices"), Collect).build())
            .out_retry(Publish::default())
            .transform(DeferredFrom);
    })
}
// --8<-- [end:out_retry]

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

    #[tokio::test(start_paused = true)]
    async fn every_copy_carries_the_stream_it_was_deferred_from() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        tb.broker::<KinesisBroker>()
            .message(&Invoice { id: 11 })
            .publish()
            .await?;
        tb.advance(PAUSE).await?;

        tb.broker::<KinesisBroker>()
            .subscriber("invoices")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.broker::<KinesisBroker>()
            .published::<Invoice>("invoices")
            .assert_called(2)
            .with(&Invoice { id: 11 })
            .with_header(DEFERRED_FROM, "invoices");
        tb.shutdown().await?;
        Ok(())
    }
}
