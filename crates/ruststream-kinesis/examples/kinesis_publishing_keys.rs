//! The partition key written as a header: a body that stays broker-agnostic names the key with
//! the portable `partition-key` header, and the publisher moves it into the record's own field.
//!
//! ```text
//! cargo test --example kinesis_publishing_keys --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    customer: String,
}

// --8<-- [start:keys]
/// The header a shipment carries: `partition-key`, which becomes the record's partition key.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct ShipmentKey {
    #[serde(rename = "partition-key")]
    customer: String,
}

/// Every shipment of one customer lands on one shard, so its consumer sees them in order.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments", headers = ShipmentKey)]
struct Shipment {
    order: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

/// The body names no broker: the plain bound moves between brokers untouched.
#[subscriber(KinesisStream::new("orders"))]
async fn ship(order: &Order, Out(shipments): Out<impl Publisher, Shipments>) -> HandlerOutcome {
    let key = ShipmentKey {
        customer: order.customer.clone(),
    };
    let shipment = Shipment { order: order.id };
    if shipments
        .message(&shipment)
        .with_headers(&key)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:keys]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(ship).out(Shipments, Publish::default()).build();
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::PARTITION_KEY_HEADER;
    use ruststream_kinesis::prelude::*;

    use super::{Order, Shipment, Shipments, app};

    #[tokio::test]
    async fn a_shipment_is_keyed_by_its_customer() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Order {
                id: 7,
                customer: "customer-42".to_owned(),
            })
            .publish()
            .await?;

        tb.out::<Shipments>().assert_called_once();
        tb.broker::<KinesisBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order: 7 })
            .with_header(PARTITION_KEY_HEADER, "customer-42");
        tb.shutdown().await?;
        Ok(())
    }
}
