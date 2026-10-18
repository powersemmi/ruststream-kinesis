//! The service of `examples/kinesis_broker_prelude.rs` written without the macros: the one glob
//! brings the descriptor, the position, the context keys and the publish policy here too.
//!
//! ```text
//! cargo test --example manual_kinesis_broker_prelude --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_kinesis::prelude::*;

/// Confirms each order with a receipt, and logs where the order sits in the stream.
struct Confirm;

impl Handle<Order, Receipt, (), KinesisContext> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, KinesisContext>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        tracing::info!(order.id, position = ?ctx.context(Position), "order confirmed");
        ready(Ok(Receipt { order: order.id }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        let orders = KinesisStream::new("orders").poll_interval(Duration::from_millis(500));
        b.include(
            subscriber(orders, Confirm)
                .reply()
                .build()
                .start_at(KinesisPosition::horizon()),
        )
        .out_reply(Publish::default())
        .partition_key("receipts-v1");
    })
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::PARTITION_KEY_HEADER;
    use ruststream_kinesis::prelude::*;

    use super::{Order, Receipt, app};

    #[tokio::test]
    async fn an_order_is_confirmed_on_the_receipts_stream() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order: 7 })
            .with_header(PARTITION_KEY_HEADER, "receipts-v1");
        tb.shutdown().await?;
        Ok(())
    }
}
