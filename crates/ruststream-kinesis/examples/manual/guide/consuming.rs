//! The service of the Consuming a stream guide, written without the macros: each handler is a
//! type with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by
//! hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_consuming --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:payments]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Payment {
    id: u64,
    currency: String,
}

impl OutgoingDestination for Payment {
    type Form = FixedName;

    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

/// Books the payments in euros and drops the rest, which another service books.
struct Book;

impl Handle<Payment, (), (), KinesisContext> for Book {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_, KinesisContext>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.currency != "EUR" {
            return ready(Err(HandlerOutcome::drop()));
        }
        let at = ctx.context(Position);
        tracing::info!(payment.id, position = ?at, "payment booked");
        ready(Ok(()))
    }
}
// --8<-- [end:payments]

// --8<-- [start:views]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct PageView {
    page: String,
}

impl OutgoingDestination for PageView {
    type Form = FixedName;

    const DESTINATION: &'static str = "page-views";
}

impl MessageHeaders for PageView {
    type Contract = NoHeaders;
}

/// Counts page views a batch at a time and settles the whole batch with one outcome.
struct Count;

impl Handle<[PageView]> for Count {
    fn handle(
        &self,
        views: &[PageView],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(views = views.len(), "page views counted");
        ready(Ok(()))
    }
}
// --8<-- [end:views]

// --8<-- [start:app]
fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("analytics", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(
                KinesisStream::new("payments")
                    .poll_interval(Duration::from_millis(250))
                    .create_if_missing(1),
                Book,
            )
            .build(),
        );
        b.include(
            subscriber(KinesisStream::new("page-views").create_if_missing(1), Count)
                .batch(nonzero!(500))
                .build(),
        );
    })
}

fn main() -> ExitCode {
    run_main(|| {
        service(
            KinesisBroker::new()
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .test_credentials(),
        )
    })
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
