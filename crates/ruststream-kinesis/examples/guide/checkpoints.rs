//! The service of the Checkpoints and delivery guarantees guide: shipments handed to a carrier,
//! a shipment without an address retired, and one for a carrier this release does not know held
//! in front of the shard's checkpoint.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_checkpoints --features testing -- run
//! ```

use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:ship]
/// The carriers this release of the service books with.
const CARRIERS: [&str; 2] = ["dhl", "ups"];

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    id: u64,
    address: String,
    carrier: String,
}

#[subscriber(KinesisStream::new("shipments").create_if_missing(1))]
async fn ship(shipment: &Shipment) -> HandlerOutcome {
    if shipment.address.is_empty() {
        tracing::warn!(shipment.id, "shipment without an address retired");
        return HandlerOutcome::drop();
    }
    if !CARRIERS.contains(&shipment.carrier.as_str()) {
        tracing::warn!(
            shipment.id,
            shipment.carrier,
            "unknown carrier, shipment held"
        );
        return HandlerOutcome::retry();
    }
    tracing::info!(shipment.id, shipment.carrier, "shipment booked");
    HandlerOutcome::ack()
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(broker, |b| {
        b.include(ship);
    })
}

#[ruststream::app]
fn app() -> impl App {
    service(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .region("us-east-1")
            .test_credentials(),
    )
}
// --8<-- [end:ship]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};

    use super::*;

    fn shipment(id: u64, address: &str, carrier: &str) -> Shipment {
        Shipment {
            id,
            address: address.to_owned(),
            carrier: carrier.to_owned(),
        }
    }

    #[tokio::test]
    async fn each_shipment_settles_with_its_outcome() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;
        let shipping = tb.broker::<KinesisBroker>();

        shipping
            .message(&shipment(1, "1 Main St", "dhl"))
            .publish()
            .await?;
        shipping
            .subscriber("shipments")
            .settled(HandlerOutcome::ack());

        shipping.message(&shipment(2, "", "ups")).publish().await?;
        shipping
            .subscriber("shipments")
            .settled(HandlerOutcome::drop());

        shipping
            .message(&shipment(3, "2 Main St", "pigeon"))
            .publish()
            .await?;
        shipping
            .subscriber("shipments")
            .settled(HandlerOutcome::retry());

        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_held_shipment_does_not_stop_the_ones_behind_it() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;
        let shipping = tb.broker::<KinesisBroker>();

        shipping
            .message(&shipment(1, "1 Main St", "pigeon"))
            .publish()
            .await?;
        shipping
            .message(&shipment(2, "2 Main St", "ups"))
            .publish()
            .await?;

        // The held shipment is not read again while this instance holds the shard.
        let ids: Vec<u64> = shipping
            .subscriber("shipments")
            .received::<Shipment>()
            .into_iter()
            .map(|shipment| shipment.id)
            .collect();
        assert_eq!(ids, [1, 2]);
        assert_eq!(
            shipping.subscriber("shipments").outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        tb.shutdown().await?;
        Ok(())
    }
}
