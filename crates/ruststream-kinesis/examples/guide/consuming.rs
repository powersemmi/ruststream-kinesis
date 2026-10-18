//! The service of the Consuming a stream guide: payments read one at a time with a shorter pause
//! between reads, and page views read in batches of up to 500.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_consuming --features testing -- run
//! ```

use std::time::Duration;

use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:payments]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    currency: String,
}

/// Books the payments in euros and drops the rest, which another service books.
#[subscriber(
    KinesisStream::new("payments")
        .poll_interval(Duration::from_millis(250))
        .create_if_missing(1)
)]
async fn book(payment: &Payment, Ctx(at): Ctx<Position>) -> HandlerOutcome {
    if payment.currency != "EUR" {
        return HandlerOutcome::drop();
    }
    tracing::info!(payment.id, position = ?at, "payment booked");
    HandlerOutcome::ack()
}
// --8<-- [end:payments]

// --8<-- [start:views]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "page-views")]
struct PageView {
    page: String,
}

/// Counts page views a batch at a time and settles the whole batch with one outcome.
#[subscriber(KinesisStream::new("page-views").create_if_missing(1))]
async fn count(views: &[PageView]) -> HandlerOutcome {
    tracing::info!(views = views.len(), "page views counted");
    HandlerOutcome::ack()
}
// --8<-- [end:views]

// --8<-- [start:app]
fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("analytics", "0.1.0")).with_broker(broker, |b| {
        b.include(book);
        b.include(count.batch(nonzero!(500)));
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
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::try_join_all;
    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn a_payment_in_euros_is_booked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        tb.broker::<KinesisBroker>()
            .message(&Payment {
                id: 1,
                currency: "EUR".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_payment_in_another_currency_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        tb.broker::<KinesisBroker>()
            .message(&Payment {
                id: 2,
                currency: "USD".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }

    // Paused, so the 50 ms a partial batch waits for more records passes at once.
    #[tokio::test(start_paused = true)]
    async fn views_published_together_arrive_in_one_batch() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;

        let views: Vec<PageView> = ["/", "/cart", "/checkout"]
            .into_iter()
            .map(|page| PageView {
                page: page.to_owned(),
            })
            .collect();
        let broker = tb.broker::<KinesisBroker>();
        try_join_all(views.iter().map(|view| broker.message(view).publish())).await?;

        tb.broker::<KinesisBroker>()
            .subscriber("page-views")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
