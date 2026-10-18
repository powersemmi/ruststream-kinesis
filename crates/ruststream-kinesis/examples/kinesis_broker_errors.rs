//! `KinesisError` where a service meets it: a record the service refuses, returned to the handler
//! that published it.
//!
//! ```text
//! cargo test --example kinesis_broker_errors --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

#[derive(OutSlot)]
#[publishes(Archived)]
struct Archive;

// --8<-- [start:handler]
use ruststream::runtime::PublishError;
use ruststream_kinesis::KinesisError;

/// Archives an invoice. An archive record over 1 MiB is refused, and the invoice is dropped with a warning
/// instead of retried, since it would be refused again.
#[subscriber(KinesisStream::new("invoices"))]
async fn archive(
    invoice: &Invoice,
    Out(archive): Out<impl Publisher<Error = KinesisError>, Archive>,
) -> HandlerOutcome {
    let archived = Archived {
        invoice: invoice.id,
        document: invoice.document.clone().into_bytes(),
    };
    let _ = archive.message(&archived).publish().await;
    let _: Option<PublishError<KinesisError>> = None;
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(archive).out(Archive, Publish::default()).build();
    })
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
    document: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices.archive")]
struct Archived {
    invoice: u64,
    /// The document's bytes, which JSON writes as an array of numbers, several bytes per byte.
    document: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Archived, Invoice, app};

    #[tokio::test]
    async fn an_invoice_the_service_takes_is_archived() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Invoice {
                id: 7,
                document: "total: 12.50".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("invoices")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<KinesisBroker>()
            .published::<Archived>("invoices.archive")
            .assert_called_once()
            .with(&Archived {
                invoice: 7,
                document: b"total: 12.50".to_vec(),
            });
        Ok(())
    }

    #[tokio::test]
    async fn an_invoice_over_the_record_limit_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Invoice {
                id: 8,
                document: "x".repeat(400 * 1024),
            })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("invoices")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<KinesisBroker>()
            .published::<Archived>("invoices.archive")
            .assert_not_called();
        Ok(())
    }
}
