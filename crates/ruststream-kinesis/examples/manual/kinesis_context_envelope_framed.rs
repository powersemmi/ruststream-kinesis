//! The service of `examples/kinesis_context_envelope_framed.rs` written without the macros: every
//! record is one JSON document of headers and payload.
//!
//! ```text
//! cargo test --example manual_kinesis_context_envelope_framed --features testing,json
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::codec::JsonCodec;
use ruststream::runtime::cli::run_main;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:framed]
/// Confirms an order to the tenant the producer named in the `x-tenant` header.
struct Confirm;

impl Handle<Order, Receipt> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt {
            order: order.id,
            tenant: ctx
                .headers()
                .get_str("x-tenant")
                .unwrap_or("unknown")
                .to_owned(),
        }))
    }
}

/// Every record the service writes and reads, the order and its receipt alike, is a JSON
/// document: `{"headers": {"x-tenant": "acme"}, "payload": [...]}`.
fn app() -> impl App {
    let broker = KinesisBroker::new().envelope(Framed::new(JsonCodec));
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(KinesisStream::new("orders"), Confirm)
                .reply()
                .build(),
        )
        .out_reply(Publish::default());
    })
}
// --8<-- [end:framed]

fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "receipts")]
struct Receipt {
    order: u64,
    tenant: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::codec::JsonCodec;
    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Order, Receipt, app};

    #[tokio::test]
    async fn the_tenant_travels_in_the_document() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<KinesisBroker<Framed<JsonCodec>>>();
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        broker
            .message(&Order { id: 7 })
            .with_headers(headers)
            .publish()
            .await?;

        broker
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt {
                order: 7,
                tenant: "acme".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }
}
