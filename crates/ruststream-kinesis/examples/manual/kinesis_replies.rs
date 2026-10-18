//! The service of `examples/kinesis_replies.rs` written without the macros: the reply body
//! implements `Handle`, and the mount site names the reply's partition key on the policy.
//!
//! ```text
//! cargo test --example manual_kinesis_replies --features testing
//! cargo run --example manual_kinesis_replies --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:replies]
use ruststream_kinesis::prelude::*;

/// A receipt always goes to the receipts stream, so the type is where that stream is named.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

/// The body names what it replies with; how that reply is published is the mount's.
struct Confirm;

impl Handle<Order, Receipt> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { order: order.id }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(
            subscriber(KinesisStream::new("orders"), Confirm)
                .reply()
                .build(),
        )
        // One key means one shard: every receipt is ordered against every other, at the cost of
        // a single shard's throughput. Leave it off to spread them instead.
        .out_reply(Publish::default())
        .partition_key("receipts-v1");
    })
}
// --8<-- [end:replies]

fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::PARTITION_KEY_HEADER;
    use ruststream_kinesis::prelude::*;

    use super::{Order, Receipt, app};

    #[tokio::test]
    async fn every_receipt_carries_the_mount_site_key() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in [7, 8] {
            tb.broker::<KinesisBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }

        tb.broker::<KinesisBroker>()
            .published::<Receipt>("receipts")
            .assert_called(2)
            .with_header(PARTITION_KEY_HEADER, "receipts-v1");
        tb.shutdown().await?;
        Ok(())
    }
}
