//! The service of `examples/kinesis_context_envelope_noop.rs` written without the macros: a
//! stream another system writes plain records to is read as it stands.
//!
//! ```text
//! cargo test --example manual_kinesis_context_envelope_noop --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:noop]
/// Counts a page view. The browser SDK writes each click as a plain JSON record.
struct Count;

impl Handle<Click> for Count {
    fn handle(
        &self,
        click: &Click,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(click.page, "page view");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("clicks", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        let clicks = KinesisStream::new("clicks").envelope(NoOp);
        b.include(subscriber(clicks, Count).build());
    })
}
// --8<-- [end:noop]

fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
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
