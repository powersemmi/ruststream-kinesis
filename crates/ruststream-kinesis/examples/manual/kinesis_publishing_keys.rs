//! The service of `examples/kinesis_publishing_keys.rs` written without the macros: the body
//! names the partition key with the portable `partition-key` header.
//!
//! ```text
//! cargo test --example manual_kinesis_publishing_keys --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
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
struct Ship;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
where
    Entry: OutEntry<Shipments, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let key = ShipmentKey {
            customer: order.customer.clone(),
        };
        let shipment = Shipment { order: order.id };
        outs.get(Shipments)
            .message(&shipment)
            .with_headers(&key)
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:keys]

fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(subscriber(KinesisStream::new("orders"), Ship).build())
            .out(Shipments, Publish::default())
            .build();
    })
}

fn main() -> ExitCode {
    run_main(app)
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
