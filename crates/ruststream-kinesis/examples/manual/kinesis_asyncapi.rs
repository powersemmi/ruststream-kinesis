//! The service of `examples/kinesis_asyncapi.rs` written without the macros: the same broker,
//! descriptor and policy, so the document is the same.
//!
//! ```text
//! cargo run --example manual_kinesis_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example manual_kinesis_asyncapi --features asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

#[derive(Debug, Deserialize, JsonSchema)]
struct Order {
    id: u64,
}

/// A receipt always goes to the receipts stream, so the type names it.
#[derive(Debug, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

// --8<-- [start:server]
/// The broker in `eu-west-1`, whose regional endpoint the document reports.
fn broker() -> KinesisBroker {
    KinesisBroker::new().region("eu-west-1")
}
// --8<-- [end:server]

// --8<-- [start:channels]
/// Confirms each order with a receipt.
struct Confirm;

impl Handle<Order, Receipt> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { id: order.id }))
    }
}

/// Reads `orders` every quarter of a second, and creates it with two shards where it is missing.
fn orders() -> KinesisStream {
    KinesisStream::new("orders")
        .poll_interval(Duration::from_millis(250))
        .create_if_missing(2)
}
// --8<-- [end:channels]

// --8<-- [start:app]
// --8<-- [start:messages]
fn service(broker: KinesisBroker) -> RustStream {
    // The label is the server's key in the document.
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_labeled("kinesis", broker, |b| {
        // Every receipt carries one key, so they share a shard and keep their order.
        b.include(subscriber(orders(), Confirm).reply().build())
            .out_reply(Publish::default())
            .partition_key("tenant-acme");
    })
}
// --8<-- [end:messages]

fn app() -> impl App {
    service(broker())
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use ruststream_kinesis::prelude::*;
    use serde_json::Value;

    use super::{broker, service};

    fn document(broker: KinesisBroker) -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(
            &build_spec(&service(broker)).to_json()?,
        )?)
    }

    /// One binding object, as the page shows it from the file.
    fn snippet(text: &str) -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(text)?)
    }

    #[test]
    fn the_server_is_the_regional_endpoint() -> Result<(), Box<dyn Error>> {
        let spec = document(broker())?;

        assert_eq!(
            spec["servers"]["kinesis"]["host"],
            "kinesis.eu-west-1.amazonaws.com"
        );
        assert_eq!(spec["servers"]["kinesis"]["protocol"], "kinesis");
        Ok(())
    }

    #[test]
    fn a_local_stack_is_reported_by_its_host_and_port() -> Result<(), Box<dyn Error>> {
        let spec = document(KinesisBroker::new().endpoint("http://localhost:4566"))?;

        assert_eq!(spec["servers"]["kinesis"]["host"], "localhost:4566");
        Ok(())
    }

    #[test]
    fn the_subscription_channel_reports_its_descriptor() -> Result<(), Box<dyn Error>> {
        let spec = document(broker())?;

        assert_eq!(
            spec["channels"]["orders"]["bindings"],
            snippet(include_str!("../../tests/snippets/asyncapi-channel.json"))?
        );
        Ok(())
    }

    #[test]
    fn the_reply_channel_reports_the_stream_it_lands_in() -> Result<(), Box<dyn Error>> {
        let spec = document(broker())?;

        assert_eq!(
            spec["channels"]["receipts"]["bindings"],
            snippet(include_str!(
                "../../tests/snippets/asyncapi-publish-channel.json"
            ))?
        );
        Ok(())
    }

    #[test]
    fn the_message_reports_the_mount_site_key() -> Result<(), Box<dyn Error>> {
        let spec = document(broker())?;

        assert_eq!(
            spec["components"]["messages"]["Receipt"]["bindings"],
            snippet(include_str!("../../tests/snippets/asyncapi-message.json"))?
        );
        Ok(())
    }
}
