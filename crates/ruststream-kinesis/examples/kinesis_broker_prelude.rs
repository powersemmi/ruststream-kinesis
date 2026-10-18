//! The prelude: one glob brings the core's prelude, the broker, its descriptor, its positions,
//! the context keys, and the publish policy under the name every broker crate uses.
//!
//! ```text
//! cargo test --example kinesis_broker_prelude --features testing
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_kinesis::prelude::*;

#[subscriber(
    KinesisStream::new("orders").poll_interval(Duration::from_millis(500)),
    start_at(KinesisPosition::horizon()),
    reply
)]
async fn confirm(order: &Order, Ctx(at): Ctx<Position>) -> Receipt {
    tracing::info!(order.id, position = ?at, "order confirmed");
    Receipt { order: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(confirm)
            .out_reply(Publish::default())
            .partition_key("receipts-v1");
    })
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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
