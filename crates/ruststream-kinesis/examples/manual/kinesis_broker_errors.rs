//! The service of `examples/kinesis_broker_errors.rs` written without the macros: a record the
//! service refuses is returned to the body that published it.
//!
//! ```text
//! cargo test --example manual_kinesis_broker_errors --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(OutSlot)]
#[publishes(Archived)]
struct Archive;

// --8<-- [start:handler]
/// Archives each invoice under its customer's reference, so one customer's invoices stay in
/// order. A record the service refuses is logged with its cause and retried; the mount site caps
/// the retries.
struct File;

impl<Entry> Handle<Invoice, (), Outs<(Entry,)>> for File
where
    Entry: OutEntry<Archive, Wire: Publisher<Options = KinesisPublishOptions>>,
{
    async fn handle(
        &self,
        invoice: &Invoice,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let sent = outs
            .get(Archive)
            .message(&Archived {
                invoice: invoice.id,
            })
            .partition_key(invoice.customer.as_str())
            .publish()
            .await;
        if let Err(err) = sent {
            // The error names the stream and the service's reason.
            tracing::warn!(subscription = "invoices", invoice.id, error = %err, "archive refused");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        // Three deliveries per invoice, then it leaves for a stream an operator reads.
        b.include(subscriber(KinesisStream::new("invoices"), File).build())
            .max_attempts(nonzero!(3))
            .dead_letter("invoices.unarchived")
            .out(Archive, Publish::default())
            .build();
    })
}

fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
    customer: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices.archive")]
struct Archived {
    invoice: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;
    use ruststream_kinesis::{KinesisError, PARTITION_KEY_HEADER};

    use super::{Archive, Archived, Invoice, app};

    #[tokio::test]
    async fn an_invoice_the_service_takes_is_archived() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Invoice {
                id: 7,
                customer: "acme".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("invoices")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.out::<Archive>().assert_called_once();
        tb.broker::<KinesisBroker>()
            .published::<Archived>("invoices.archive")
            .assert_called_once()
            .with(&Archived { invoice: 7 })
            .with_header(PARTITION_KEY_HEADER, "acme");
        Ok(())
    }

    /// The service takes a partition key of 256 characters at most, so this archive record is
    /// refused on every delivery, and the invoice leaves for the dead-letter stream.
    #[tokio::test]
    async fn a_refused_archive_ends_on_the_dead_letter_stream() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let invoice = Invoice {
            id: 8,
            customer: "c".repeat(300),
        };
        tb.broker::<KinesisBroker>()
            .message(&invoice)
            .publish()
            .await?;
        tb.settle().await?;

        tb.broker::<KinesisBroker>()
            .subscriber("invoices")
            .assert_called(3)
            .settled(HandlerOutcome::retry());
        tb.broker::<KinesisBroker>()
            .published::<Archived>("invoices.archive")
            .assert_not_called();
        tb.broker::<KinesisBroker>()
            .published::<Invoice>("invoices.unarchived")
            .assert_called_once()
            .with(&invoice);
        Ok(())
    }

    /// Reads invoices from a stream of zero shards, which the descriptor refuses before any
    /// request.
    struct Unprovisioned;

    impl Handle<Invoice> for Unprovisioned {
        fn handle(
            &self,
            invoice: &Invoice,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(invoice.id, "invoice read");
            ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn a_failed_startup_carries_the_kinesis_error() {
        let app = RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(
            KinesisBroker::new(),
            |b| {
                let invoices = KinesisStream::new("invoices").create_if_missing(0);
                b.include(subscriber(invoices, Unprovisioned).build());
            },
        );

        let failed = TestApp::start(app).await.err();
        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(kinesis) = err.downcast_ref::<KinesisError>() {
                assert!(matches!(kinesis, KinesisError::Invalid(_)), "{kinesis}");
                return;
            }
            cause = err.source();
        }
        panic!("startup did not fail with a KinesisError: {failed:?}");
    }
}
