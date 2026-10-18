//! A minimal Kinesis service: one `#[subscriber]` handler on the stream `orders`, on a broker
//! whose region and credentials resolve from the environment when the service starts.
//!
//! ```text
//! cargo test --example kinesis_service --features testing
//! cargo run --example kinesis_service --features testing -- run
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_kinesis::prelude::*;

// --8<-- [start:handler]
/// Reads every shard of the stream `orders`.
#[subscriber(KinesisStream::new("orders"))]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(place);
    })
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn an_order_on_the_stream_is_placed() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
