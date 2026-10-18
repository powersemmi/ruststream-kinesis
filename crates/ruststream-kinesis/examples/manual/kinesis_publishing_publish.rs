//! The service of `examples/kinesis_publishing_publish.rs` written without the macros: the reply
//! goes out through the broker's default policy, and the slot names the policy.
//!
//! ```text
//! cargo test --example manual_kinesis_publishing_publish --features testing
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
}

/// A confirmation always goes to the confirmations stream, so the type names it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
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
struct Confirm;

impl<Entry> Handle<Order, Confirmation, Outs<(Entry,)>> for Confirm
where
    Entry: OutEntry<Shipments, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<Confirmation, HandlerOutcome> {
        let shipment = Shipment { order: order.id };
        if let Err(err) = outs.get(Shipments).message(&shipment).publish().await {
            tracing::warn!(subscription = "orders", order.id, error = %err, "shipment not published");
        }
        Ok(Confirmation { order: order.id })
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        // The reply names no policy and takes the broker's default; the slot names one.
        b.include(
            subscriber(KinesisStream::new("orders"), Confirm)
                .reply()
                .build(),
        )
        .out(Shipments, Publish::default())
        .build();
    })
}
// --8<-- [end:publish]

fn main() -> ExitCode {
    run_main(app)
}

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
