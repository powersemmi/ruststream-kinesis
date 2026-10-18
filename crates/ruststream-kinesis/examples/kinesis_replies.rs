//! A reply's partition key, named at the mount site: the reply type declares the stream it goes
//! to, and `.out_reply(..)` names the policy it is published through, with this crate's
//! `partition_key` setting after it.
//!
//! ```text
//! cargo test --example kinesis_replies --features testing
//! cargo run --example kinesis_replies --features testing -- run
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:replies]
use ruststream_kinesis::prelude::*;

/// A receipt always goes to the receipts stream, so the type is where that stream is named.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
}

/// The handler names what it replies with; how that reply is published is the mount's.
#[subscriber(KinesisStream::new("orders"), reply)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { order: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        // One key means one shard: every receipt is ordered against every other, at the cost of
        // a single shard's throughput. Leave it off to spread them instead.
        b.include(confirm)
            .out_reply(Publish::default())
            .partition_key("receipts-v1");
    })
}
// --8<-- [end:replies]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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
