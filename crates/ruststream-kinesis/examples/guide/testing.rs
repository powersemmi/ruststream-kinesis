//! The service the Testing guide tests: card payments keyed by account, a receipt for each, and a
//! review stream for the payments the bank keeps holding.
//!
//! ```text
//! cargo test --example guide_testing --features testing
//! ```

use std::time::Duration;

use ruststream_kinesis::PARTITION_KEY_HEADER;
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:service]
/// The largest payment the bank settles at once; a larger one is held.
const INSTANT_LIMIT_CENTS: u64 = 100_000;

/// How long a held payment waits before the service asks the bank again.
const HOLD: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "card-payments")]
struct Payment {
    id: u64,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "card-receipts")]
struct Receipt {
    payment: u64,
    account: String,
}

/// Charges a payment to the account its partition key names.
#[subscriber(KinesisStream::new("card-payments").create_if_missing(1), reply)]
async fn charge(payment: &Payment, ctx: &mut Context<'_>) -> Result<Receipt, HandlerOutcome> {
    if payment.amount_cents == 0 {
        tracing::warn!(payment.id, "payment without an amount dropped");
        return Err(HandlerOutcome::drop());
    }
    if payment.amount_cents > INSTANT_LIMIT_CENTS {
        tracing::info!(payment.id, "payment held by the bank");
        return Err(HandlerOutcome::retry_after(HOLD));
    }
    let account = ctx
        .headers()
        .get_str(PARTITION_KEY_HEADER)
        .unwrap_or_default()
        .to_owned();
    tracing::info!(payment.id, account, "payment charged");
    Ok(Receipt {
        payment: payment.id,
        account,
    })
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(charge)
            .max_attempts(nonzero!(3u32))
            .dead_letter("card-payments.review")
            .out_reply(Publish::default());
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
// --8<-- [end:service]

#[cfg(test)]
#[path = "../support/kinesis_live.rs"]
mod support;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};

    use super::*;

    fn keyed(account: &str) -> HeaderMap {
        let mut key = HeaderMap::new();
        key.insert(PARTITION_KEY_HEADER, account.to_owned());
        key
    }

    // --8<-- [start:in_process]
    #[tokio::test]
    async fn a_keyed_payment_gets_a_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;
        a_receipt_names_the_account_of_the_key(tb).await
    }

    async fn a_receipt_names_the_account_of_the_key(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<KinesisBroker>()
            .message(&Payment {
                id: 1,
                amount_cents: 2_500,
            })
            .with_headers(keyed("acct-7"))
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<KinesisBroker>()
            .published::<Receipt>("card-receipts")
            .assert_called_once()
            .with(&Receipt {
                payment: 1,
                account: "acct-7".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:in_process]

    // --8<-- [start:dropped]
    #[tokio::test]
    async fn a_payment_without_an_amount_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        tb.broker::<KinesisBroker>()
            .message(&Payment {
                id: 2,
                amount_cents: 0,
            })
            .with_headers(keyed("acct-7"))
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<KinesisBroker>()
            .published::<Receipt>("card-receipts")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:dropped]

    // --8<-- [start:held]
    #[tokio::test(start_paused = true)]
    async fn a_payment_the_bank_keeps_holding_goes_to_review() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;
        let held = Payment {
            id: 3,
            amount_cents: 250_000,
        };

        tb.broker::<KinesisBroker>()
            .message(&held)
            .with_headers(keyed("acct-7"))
            .publish()
            .await?;
        tb.broker::<KinesisBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(HOLD));

        // The delay is a copy of the record, published back to the stream after `HOLD`.
        tb.advance(HOLD).await?;
        tb.broker::<KinesisBroker>()
            .published::<Payment>("card-payments")
            .assert_called(2)
            .with(&held)
            .with_header(RETRY_COUNT_HEADER, "1")
            .with_header(PARTITION_KEY_HEADER, "acct-7");

        // The third delivery is the last one: it goes to the review stream as it arrived.
        tb.advance(HOLD).await?;
        assert_eq!(
            tb.broker::<KinesisBroker>()
                .subscriber("card-payments")
                .outcomes(),
            [Outcome::Nack, Outcome::Nack, Outcome::Nack],
        );
        tb.broker::<KinesisBroker>()
            .published::<Payment>("card-payments.review")
            .assert_called_once()
            .with(&held);
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:held]

    // --8<-- [start:live]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_keyed_payment_gets_a_receipt_on_a_stack() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = support::endpoint() else {
            return Ok(());
        };
        // The service reads `card-payments` and creates it; the receipts are only published.
        support::ensure_stream(&support::config(&endpoint).await, "card-receipts", 1).await;
        let broker = KinesisBroker::new()
            .endpoint(endpoint)
            .region("us-east-1")
            .test_credentials();
        let tb = TestApp::start_live(service(broker)).await?;
        a_receipt_names_the_account_of_the_key(tb).await
    }
    // --8<-- [end:live]
}
