//! The service of `examples/kinesis_publishing_step.rs` written without the macros: a body that
//! names the key bounds its slot's wire on `KinesisPublishOptions`.
//!
//! ```text
//! cargo test --example manual_kinesis_publishing_step --features testing
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
    tenant: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.journal")]
struct Journalled {
    order: u64,
}

// --8<-- [start:step]
/// The slot the journal entries leave through.
#[derive(OutSlot)]
#[publishes(Journalled)]
struct Journal;

/// Every entry of one tenant lands on one shard, so the journal keeps each tenant's order. The
/// bound on the options type puts the step within reach, and says that this body runs on
/// Kinesis.
struct Record;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Record
where
    Entry: OutEntry<Journal, Wire: Publisher<Options = KinesisPublishOptions>>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        outs.get(Journal)
            .message(&Journalled { order: order.id })
            .partition_key(order.tenant.as_str())
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:step]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(subscriber(KinesisStream::new("orders"), Record).build())
            .out(Journal, Publish::default())
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

    use super::{Journal, Journalled, Order, app};

    #[tokio::test]
    async fn each_entry_is_keyed_by_its_tenant() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Order {
                id: 7,
                tenant: "tenant-acme".to_owned(),
            })
            .publish()
            .await?;

        tb.out::<Journal>()
            .assert_called_once()
            .with_options(&KinesisPublishOptions {
                partition_key: Some("tenant-acme".to_owned()),
            });
        tb.broker::<KinesisBroker>()
            .published::<Journalled>("orders.journal")
            .assert_called_once()
            .with(&Journalled { order: 7 })
            .with_header(PARTITION_KEY_HEADER, "tenant-acme");
        tb.shutdown().await?;
        Ok(())
    }
}
