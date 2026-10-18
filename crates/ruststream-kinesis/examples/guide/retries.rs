//! The refunds service of the Retries and dead-letter streams guide: a busy card processor is
//! asked again later, a silent one at once, five deliveries at most, then the dead-letter stream.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_retries --features testing -- run
//! ```

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

/// Why the card processor refused a refund.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// The processor's rate limit is used up for now.
    Busy,
    /// The processor did not answer.
    Unavailable,
    /// The card account is closed; another try gets the same answer.
    Declined,
}

/// Stands in for the card processor's client.
#[derive(Debug, Default)]
struct Processor {
    /// How many more calls find the processor busy.
    busy_for: Mutex<u32>,
    offline: AtomicBool,
}

impl Processor {
    async fn refund(&self, refund: &Refund) -> Result<(), Refusal> {
        tokio::task::yield_now().await;
        if self.offline.load(Ordering::Relaxed) {
            return Err(Refusal::Unavailable);
        }
        if refund.card.is_empty() {
            return Err(Refusal::Declined);
        }
        let busy = {
            let mut busy_for = self.busy_for.lock().unwrap_or_else(PoisonError::into_inner);
            let busy = *busy_for > 0;
            if busy {
                *busy_for -= 1;
            }
            busy
        };
        if busy { Err(Refusal::Busy) } else { Ok(()) }
    }
}

/// The clients the handlers share, opened once at startup.
#[derive(Debug, Default, Clone, FromRef)]
struct Clients {
    processor: Arc<Processor>,
}

// --8<-- [start:refund]
/// How long a busy processor asks its callers to wait.
const BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "refunds")]
struct Refund {
    id: u64,
    card: String,
    amount_cents: u64,
}

#[subscriber(KinesisStream::new("refunds").create_if_missing(1))]
async fn process_refund(
    refund: &Refund,
    State(processor): State<Arc<Processor>>,
) -> HandlerOutcome {
    match processor.refund(refund).await {
        Ok(()) => HandlerOutcome::ack(),
        // Past the rate limit: a delivery right now would be refused again.
        Err(Refusal::Busy) => HandlerOutcome::retry_after(BACKOFF),
        // No answer: try again at once, up to the attempt cap.
        Err(Refusal::Unavailable) => HandlerOutcome::retry(),
        Err(Refusal::Declined) => {
            tracing::warn!(refund.id, "card declined, refund dropped");
            HandlerOutcome::drop()
        }
    }
}
// --8<-- [end:refund]

// --8<-- [start:dead_letters]
#[subscriber(KinesisStream::new("refunds.review").create_if_missing(1))]
async fn review(refund: &Refund, ctx: &mut Context<'_>) -> HandlerOutcome {
    let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
    tracing::error!(refund.id, deliveries, "refund needs a person to look at it");
    HandlerOutcome::ack()
}
// --8<-- [end:dead_letters]

// --8<-- [start:app]
fn service(broker: KinesisBroker) -> impl App<State = Clients> {
    RustStream::new(AppInfo::new("refunds", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Clients::default()))
        .with_broker(broker, |b| {
            // Five deliveries of one refund, the first included; then the dead-letter stream.
            b.include(process_refund)
                .max_attempts(nonzero!(5u32))
                .dead_letter("refunds.review");
            b.include(review);
        })
}

#[ruststream::app]
fn app() -> impl App<State = Clients> {
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
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use ruststream::HeaderMap;
    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_kinesis::PARTITION_KEY_HEADER;

    use super::*;

    fn refund(id: u64) -> Refund {
        Refund {
            id,
            card: "4242".to_owned(),
            amount_cents: 1500,
        }
    }

    fn with_processor(busy_for: u32, offline: bool) -> Clients {
        Clients {
            processor: Arc::new(Processor {
                busy_for: Mutex::new(busy_for),
                offline: AtomicBool::new(offline),
            }),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_processor_is_asked_again_after_the_backoff() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(service(KinesisBroker::new()), |_| with_processor(1, false))
            .await?;
        let mut key = HeaderMap::new();
        key.insert(PARTITION_KEY_HEADER, "card-4242");

        tb.broker::<KinesisBroker>()
            .message(&refund(1))
            .with_headers(key)
            .publish()
            .await?;
        tb.broker::<KinesisBroker>()
            .subscriber("refunds")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(BACKOFF));

        tb.advance(BACKOFF).await?;

        assert_eq!(
            tb.broker::<KinesisBroker>()
                .subscriber("refunds")
                .outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        // The copy counts the delivery and keeps the record's key, so it lands on its shard.
        tb.broker::<KinesisBroker>()
            .published::<Refund>("refunds")
            .assert_called(2)
            .with(&refund(1))
            .with_header(RETRY_COUNT_HEADER, "1")
            .with_header(PARTITION_KEY_HEADER, "card-4242");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_processor_sends_the_refund_to_review() -> Result<(), Box<dyn Error>> {
        let tb =
            TestApp::with_state(service(KinesisBroker::new()), |_| with_processor(0, true)).await?;

        tb.broker::<KinesisBroker>()
            .message(&refund(2))
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("refunds")
            .assert_called(5);
        tb.broker::<KinesisBroker>()
            .published::<Refund>("refunds.review")
            .assert_called_once()
            .with(&refund(2))
            .with_header(RETRY_COUNT_HEADER, "5");
        tb.broker::<KinesisBroker>()
            .subscriber("refunds.review")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_declined_card_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;
        let declined = Refund {
            card: String::new(),
            ..refund(3)
        };

        tb.broker::<KinesisBroker>()
            .message(&declined)
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("refunds")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<KinesisBroker>()
            .published::<Refund>("refunds.review")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
