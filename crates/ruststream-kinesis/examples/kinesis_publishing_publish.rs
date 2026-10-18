//! The publish policy: a reply goes out through `KinesisPublish`, the broker's default policy, so
//! a mount that names no publisher still publishes the reply, one `PutRecord` per record.
//!
//! ```text
//! cargo test --example kinesis_publishing_publish --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// A confirmation always goes to the confirmations stream, so the type names it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    order: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

/// What the warehouse reads; the slot's publish names the stream.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order: u64,
}

// --8<-- [start:publish]
/// Confirms each order with its return value and hands it to the warehouse through a slot.
#[subscriber(KinesisStream::new("orders"), reply)]
async fn confirm(order: &Order, Out(shipments): Out<impl Publisher, Shipments>) -> Confirmation {
    let shipment = Shipment { order: order.id };
    if let Err(err) = shipments.message(&shipment).publish().await {
        tracing::warn!(subscription = "orders", order.id, error = %err, "shipment not published");
    }
    Confirmation { order: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        // The reply names no policy and takes the broker's default; the slot names one.
        b.include(confirm)
            .out(Shipments, Publish::default())
            .build();
    })
}
// --8<-- [end:publish]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Confirmation, Order, Shipment, app};

    #[tokio::test]
    async fn an_order_is_confirmed_and_shipped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { order: 7 })
            .assert_options_default();
        tb.broker::<KinesisBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
