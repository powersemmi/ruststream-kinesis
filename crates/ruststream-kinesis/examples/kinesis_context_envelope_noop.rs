//! No envelope: a stream another system writes plain records to is read as it stands.
//!
//! ```text
//! cargo test --example kinesis_context_envelope_noop --features testing
//! ```

use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:noop]
/// Counts a page view. The browser SDK writes each click as a plain JSON record.
#[subscriber(KinesisStream::new("clicks"))]
async fn count(click: &Click) -> HandlerOutcome {
    tracing::info!(click.page, "page view");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("clicks", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(count.envelope(NoOp));
    })
}
// --8<-- [end:noop]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "clicks")]
struct Click {
    page: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Click, app};

    #[tokio::test]
    async fn a_plain_record_is_the_payload() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let click = Click {
            page: "/pricing".to_owned(),
        };
        tb.broker::<KinesisBroker>()
            .message(&click)
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("clicks")
            .assert_called_once()
            .with(&click)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
