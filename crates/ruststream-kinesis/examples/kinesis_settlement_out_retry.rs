//! `out_retry`: the publisher the retry copies leave through, replaced at the mount site so a
//! transform stamps every copy. Each scenario is a test, and `cargo run` starts the service against
//! a local stack.
//!
//! ```text
//! cargo test --example kinesis_settlement_out_retry --features testing
//! ```

use std::time::Duration;

use ruststream::runtime::{
    ForReply, Outgoing, PublishContext, PublishTransform, RETRY_COUNT_HEADER, Reads,
};
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
}

/// The header every retry copy carries: the stream the record was deferred from.
const DEFERRED_FROM: &str = "x-deferred-from";

/// How long an invoice the bank has not confirmed waits for its next delivery.
const PAUSE: Duration = Duration::from_secs(60);

/// Waits for the bank's confirmation, which the first delivery never has.
#[subscriber(KinesisStream::new("invoices"))]
async fn collect(_invoice: &Invoice, ctx: &mut Context<'_, KinesisContext>) -> HandlerOutcome {
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        return HandlerOutcome::retry_after(PAUSE);
    }
    HandlerOutcome::ack()
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
        b.include(collect)
            .out_retry(Publish::default())
            .transform(DeferredFrom);
    })
}
// --8<-- [end:out_retry]

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
