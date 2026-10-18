//! Testing a Kinesis service in process: `TestApp::start` connects the production app's
//! `KinesisBroker` without a server, and the in-process stream refuses what the service refuses.
//!
//! ```text
//! cargo test --example kinesis_testing --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    account: String,
    amount: u64,
}

/// What the ledger stream holds: one entry per accepted payment.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger")]
struct Entry {
    amount: u64,
}

#[derive(OutSlot)]
#[publishes(Entry)]
struct Ledger;

/// Books a payment on the ledger under its account, so one account's entries stay in order.
#[subscriber(KinesisStream::new("payments"))]
async fn accept(
    payment: &Payment,
    Out(ledger): Out<impl Publisher<Options = KinesisPublishOptions>, Ledger>,
) -> HandlerOutcome {
    let sent = ledger
        .message(&Entry {
            amount: payment.amount,
        })
        .partition_key(payment.account.as_str())
        .publish()
        .await;
    if let Err(err) = sent {
        tracing::warn!(subscription = "payments", error = %err, "entry not booked");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(accept).out(Ledger, Publish::default()).build();
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::PARTITION_KEY_HEADER;
    use ruststream_kinesis::prelude::*;

    use super::{Entry, Payment, app};

    fn payment(account: &str) -> Payment {
        Payment {
            account: account.to_owned(),
            amount: 100,
        }
    }

    // --8<-- [start:testapp]
    #[tokio::test]
    async fn a_payment_is_booked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&payment("acc-1"))
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&payment("acc-1"))
            .settled(HandlerOutcome::ack());
        tb.broker::<KinesisBroker>()
            .published::<Entry>("ledger")
            .assert_called_once()
            .with(&Entry { amount: 100 })
            .with_header(PARTITION_KEY_HEADER, "acc-1");
        Ok(())
    }
    // --8<-- [end:testapp]

    // --8<-- [start:refusals]
    #[tokio::test]
    async fn an_entry_the_service_refuses_is_refused() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // A partition key is 1 to 256 characters, so the entry for this account is refused.
        let unbookable = payment(&"a".repeat(300));
        tb.broker::<KinesisBroker>()
            .message(&unbookable)
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        tb.broker::<KinesisBroker>()
            .published::<Entry>("ledger")
            .assert_not_called();
        Ok(())
    }
    // --8<-- [end:refusals]
}
