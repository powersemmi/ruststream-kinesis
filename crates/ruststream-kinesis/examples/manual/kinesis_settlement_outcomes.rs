//! The service of `examples/kinesis_settlement_outcomes.rs` written without the macros: the outcome
//! on the error side settles the record: an ack moves the checkpoint, a drop retires the record, a
//! plain retry holds the checkpoint.
//!
//! ```text
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
//!     cargo test --example manual_kinesis_settlement_outcomes --features testing -- --test-threads=1
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Entry {
    id: u64,
    amount: i64,
}

// `#[outgoing(name = "ledger")]` by hand: the type fixes its destination.
impl OutgoingDestination for Entry {
    type Form = FixedName;

    const DESTINATION: &'static str = "ledger";
}

impl MessageHeaders for Entry {
    type Contract = NoHeaders;
}

// --8<-- [start:outcomes]
/// Books each entry. An empty entry is retired; a refund waits for the payment it returns, and
/// holds the shard's checkpoint in front of it until then.
struct Book;

impl Handle<Entry> for Book {
    fn handle(
        &self,
        entry: &Entry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(match entry.amount {
            0 => Err(HandlerOutcome::drop()),
            amount if amount < 0 => Err(HandlerOutcome::retry()),
            amount => {
                tracing::info!(entry.id, amount, "booked");
                Ok(())
            }
        })
    }
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(KinesisStream::new("ledger"), Book).build());
    })
}
// --8<-- [end:outcomes]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let app = service(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
    );
    app.run().await?;
    Ok(())
}

#[cfg(test)]
#[path = "../support/kinesis_live.rs"]
mod support;

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::MemoryLeaseStore;

    use super::*;

    #[tokio::test]
    async fn each_entry_settles_with_its_outcome() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(KinesisBroker::new())).await?;
        let ledger = tb.broker::<KinesisBroker>();

        ledger
            .message(&Entry { id: 1, amount: 40 })
            .publish()
            .await?;
        ledger.subscriber("ledger").settled(HandlerOutcome::ack());

        ledger
            .message(&Entry { id: 2, amount: 0 })
            .publish()
            .await?;
        ledger.subscriber("ledger").settled(HandlerOutcome::drop());

        ledger
            .message(&Entry { id: 3, amount: -15 })
            .publish()
            .await?;
        ledger.subscriber("ledger").settled(HandlerOutcome::retry());

        // A shard cannot requeue one record, so the held entry does not come back in this run.
        ledger.subscriber("ledger").assert_called(3);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_held_entry_is_read_again_after_a_restart() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = support::endpoint() else {
            return Ok(());
        };
        support::ensure_stream(&support::config(&endpoint).await, "ledger", 1).await;
        // The checkpoints outlive the first run, as a durable store's do.
        let leases = Arc::new(MemoryLeaseStore::new());
        let instance = || {
            support::broker(&endpoint)
                .lease_store(leases.clone())
                .owner_id("ledger-1")
        };

        let first = TestApp::start_live(service(instance())).await?;
        for (id, amount) in [(1, 40), (2, -15), (3, 25)] {
            first
                .broker::<KinesisBroker>()
                .message(&Entry { id, amount })
                .publish()
                .await?;
        }
        first.shutdown().await?;

        let second = TestApp::start_live(service(instance())).await?;
        second
            .broker::<KinesisBroker>()
            .message(&Entry { id: 4, amount: 10 })
            .publish()
            .await?;

        // The checkpoint stopped in front of entry 2, so the restarted service reads from it,
        // ahead of the entry published after the restart.
        let replayed = second
            .broker::<KinesisBroker>()
            .subscriber("ledger")
            .received::<Entry>();
        assert_eq!(replayed[0], Entry { id: 2, amount: -15 });
        second.shutdown().await?;
        Ok(())
    }
}
