//! The service of the Replay and seeking guide: read models rebuilt from the retained log, a
//! review that starts at a moment, and handlers that move their own subscription.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_replay --features testing -- run
//! ```

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ruststream_kinesis::prelude::*;
use serde::Deserialize;

// --8<-- [start:projection]
/// One posting of the ledger. `revision` is the ledger's revision: a correction of past
/// postings is published as a new revision, and a read model meeting one rebuilds itself.
#[derive(Debug, PartialEq, Deserialize)]
struct Posting {
    id: u64,
    account: String,
    amount_cents: i64,
    revision: u32,
}

/// A read model kept in memory: the balance of every account.
#[derive(Debug, Default)]
struct Projection {
    revision: u32,
    balances: HashMap<String, i64>,
}

impl Projection {
    /// Applies `posting`, or clears the model and answers `false` when the posting belongs to a
    /// newer revision, which the model has to be rebuilt for from the start of the log.
    fn apply(&mut self, posting: &Posting) -> bool {
        if posting.revision > self.revision {
            self.revision = posting.revision;
            self.balances.clear();
            return false;
        }
        *self.balances.entry(posting.account.clone()).or_default() += posting.amount_cents;
        true
    }
}

/// The service's state: one read model per subscription that builds one.
#[derive(Clone, Default)]
struct Projections {
    balances: Arc<Mutex<Projection>>,
    spend: Arc<Mutex<Projection>>,
}
// --8<-- [end:projection]

// --8<-- [start:balance]
#[subscriber(
    KinesisStream::new("ledger").create_if_missing(1),
    start_at(KinesisPosition::horizon())
)]
async fn project_balance(
    posting: &Posting,
    ctx: &mut Context<'_, KinesisContext, Projections>,
    Ctx(seeker): Ctx<SeekHandle>,
) -> HandlerOutcome {
    let applied = ctx
        .state()
        .balances
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .apply(posting);
    if applied {
        tracing::info!(posting.id, "balance updated");
        return HandlerOutcome::ack();
    }
    tracing::info!(
        posting.id,
        posting.revision,
        "new revision, rebuilding the balances"
    );
    if seeker.seek(KinesisPosition::horizon()).await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:balance]

// --8<-- [start:batch]
#[subscriber(KinesisStream::new("card-postings").create_if_missing(1))]
async fn project_spend(
    postings: &[Posting],
    ctx: &mut Context<'_, KinesisBatchContext, Projections>,
) -> HandlerOutcome {
    let applied = {
        let mut spend = ctx
            .state()
            .spend
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        postings.iter().all(|posting| spend.apply(posting))
    };
    if applied {
        tracing::info!(postings = postings.len(), "card spend updated");
        return HandlerOutcome::ack();
    }
    tracing::info!("new revision, rebuilding the card spend");
    if ctx
        .context(SeekHandle)
        .seek(KinesisPosition::horizon())
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:batch]

// --8<-- [start:payouts]
/// A payout as the payments service publishes it.
#[derive(Debug, PartialEq, Deserialize)]
struct Payout {
    id: u64,
    amount_cents: i64,
}

#[subscriber(KinesisStream::new("payouts").create_if_missing(1))]
async fn review_payout(payout: &Payout) -> HandlerOutcome {
    tracing::info!(payout.id, payout.amount_cents, "payout reviewed");
    HandlerOutcome::ack()
}
// --8<-- [end:payouts]

// --8<-- [start:service]
/// Milliseconds since the epoch, the unit of a Kinesis arrival timestamp.
fn epoch_millis(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH).map_or(0, |since| {
        u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
    })
}

fn service(broker: KinesisBroker, review_from: SystemTime) -> impl App<State = Projections> {
    RustStream::new(AppInfo::new("ledger", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Projections::default()))
        .with_broker(broker, |b| {
            b.include(project_balance);
            b.include(project_spend.batch(nonzero!(500usize)));
            b.include(
                review_payout.start_at(KinesisPosition::timestamp(epoch_millis(review_from))),
            );
        })
}

/// The review reads the payouts of the last day on every start, the stream's default retention.
const REVIEW_WINDOW: Duration = Duration::from_hours(24);

#[ruststream::app]
fn app() -> impl App<State = Projections> {
    let review_from = SystemTime::now() - REVIEW_WINDOW;
    service(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .region("us-east-1")
            .test_credentials(),
        review_from,
    )
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::SystemTime;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Projections, app, service};

    /// A posting as the ledger publishes it.
    #[derive(Serialize, Outgoing)]
    struct Posted {
        id: u64,
        account: &'static str,
        amount_cents: i64,
        revision: u32,
    }

    fn posted(id: u64, revision: u32) -> Posted {
        Posted {
            id,
            account: "acc-1",
            amount_cents: 100,
            revision,
        }
    }

    /// A payout as the payments service publishes it.
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "payouts")]
    struct PaidOut {
        id: u64,
        amount_cents: i64,
    }

    /// The ids `stream`'s handler received, in order.
    fn ids(tb: &TestApp<Projections>, stream: &str) -> Vec<u64> {
        #[derive(Deserialize)]
        struct Id {
            id: u64,
        }
        tb.broker::<KinesisBroker>()
            .subscriber(stream)
            .received::<Id>()
            .into_iter()
            .map(|posting| posting.id)
            .collect()
    }

    #[tokio::test]
    async fn a_new_revision_rebuilds_the_balances_from_the_horizon() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (id, revision) in [(1, 0), (2, 0), (3, 1)] {
            tb.broker::<KinesisBroker>()
                .message(&posted(id, revision))
                .to("ledger")
                .publish()
                .await?;
        }

        assert_eq!(ids(&tb, "ledger"), [1, 2, 3, 1, 2, 3]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_batch_with_a_new_revision_rebuilds_the_spend() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (id, revision) in [(1, 0), (2, 1)] {
            tb.broker::<KinesisBroker>()
                .message(&posted(id, revision))
                .to("card-postings")
                .publish()
                .await?;
        }

        assert_eq!(ids(&tb, "card-postings"), [1, 2, 1, 2]);
        tb.broker::<KinesisBroker>()
            .subscriber("card-postings")
            .assert_batch_sizes(&[1, 1, 2]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_review_reads_the_payouts_published_after_its_moment() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::start(service(KinesisBroker::new(), SystemTime::now())).await?;

        tb.broker::<KinesisBroker>()
            .message(&PaidOut {
                id: 1,
                amount_cents: 500,
            })
            .publish()
            .await?;

        assert_eq!(ids(&tb, "payouts"), [1]);
        tb.shutdown().await?;
        Ok(())
    }
}
