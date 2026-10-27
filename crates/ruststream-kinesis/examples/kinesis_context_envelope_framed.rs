//! The framed envelope: every record is one JSON document of headers and payload, which a
//! consumer in another language reads with its own JSON library.
//!
//! ```text
//! cargo test --example kinesis_context_envelope_framed --features testing,json
//! ```

use ruststream::codec::JsonCodec;
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:framed]
/// Confirms an order to the tenant the producer named in the `x-tenant` header.
#[subscriber(KinesisStream::new("orders"), reply)]
async fn confirm(order: &Order, ctx: &mut Context<'_>) -> Receipt {
    Receipt {
        order: order.id,
        tenant: ctx
            .headers()
            .get_str("x-tenant")
            .unwrap_or("unknown")
            .to_owned(),
    }
}

/// Every record the service writes and reads, the order and its receipt alike, is a JSON
/// document: `{"headers":{"x-tenant":"acme"},"payload":{"id":7}}`.
#[ruststream::app]
fn app() -> impl App {
    let broker = KinesisBroker::new().envelope(Framed::new(JsonCodec));
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(confirm);
    })
}
// --8<-- [end:framed]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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
