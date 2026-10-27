//! The service of `examples/kinesis_context_envelope_custom.rs` written without the macros: an
//! envelope of the service's own, the layout a legacy producer already writes.
//!
//! ```text
//! cargo test --example manual_kinesis_context_envelope_custom --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::{BytesMut, HeaderMap};
use ruststream_kinesis::envelope::{Body, EnvelopeError, EnvelopeHeaders};
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:custom]
/// One `tenant=<name>` line, then the payload. A record without the line is payload alone.
#[derive(Debug, Clone, Copy, Default)]
struct TenantLine;

const PREFIX: &[u8] = b"tenant=";

impl TenantLine {
    /// The tenant and where the payload starts, when the record begins with the line.
    fn split(record: &[u8]) -> Option<(&[u8], usize)> {
        let line = record.strip_prefix(PREFIX)?;
        let end = line.iter().position(|byte| *byte == b'\n')?;
        Some((&line[..end], PREFIX.len() + end + 1))
    }
}

impl Envelope for TenantLine {
    const NAME: &'static str = "tenant-line";

    fn seal(
        &self,
        headers: EnvelopeHeaders<'_>,
        payload: BytesMut,
    ) -> Result<Vec<u8>, EnvelopeError> {
        let mut record = Vec::with_capacity(payload.len() + 32);
        for (name, value) in headers.iter() {
            if name != "tenant" {
                return Err(EnvelopeError::unsupported_header(name));
            }
            record.extend_from_slice(PREFIX);
            record.extend_from_slice(value);
            record.push(b'\n');
        }
        record.extend_from_slice(&payload);
        Ok(record)
    }

    fn body(&self, record: &[u8]) -> Body {
        Self::split(record).map_or(Body::Whole, |(_, start)| Body::Within(start..record.len()))
    }

    fn read_headers(&self, record: &[u8], headers: &mut HeaderMap) {
        if let Some((tenant, _)) = Self::split(record) {
            headers.insert("tenant", tenant.to_vec());
        }
    }
}

/// Bills an order to the tenant the legacy producer named.
struct Bill;

impl Handle<Order> for Bill {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let Some(tenant) = ctx.headers().get_str("tenant") else {
            return ready(Err(HandlerOutcome::drop()));
        };
        tracing::info!(order.id, tenant, "billed");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        let orders = KinesisStream::new("orders").envelope(TenantLine);
        b.include(subscriber(orders, Bill).build());
    })
}
// --8<-- [end:custom]

fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
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

    /// A record exactly as the legacy producer writes it.
    #[derive(Outgoing, Serialized)]
    #[outgoing(name = "orders")]
    struct Legacy(Vec<u8>);

    #[tokio::test]
    async fn the_tenant_line_reaches_the_handler_as_a_header() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Legacy(b"tenant=acme\n{\"id\":7}".to_vec()))
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

    #[tokio::test]
    async fn an_order_without_the_tenant_line_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
