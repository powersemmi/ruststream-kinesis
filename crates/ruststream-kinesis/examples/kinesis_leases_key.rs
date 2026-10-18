//! `LeaseKey`: the name of one shard of one stream, which a lease store of your own keys its rows
//! by. The scenario runs against a stand, since the in-process transport consults no store.
//!
//! ```text
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
//!     cargo test --example kinesis_leases_key --features testing -- --test-threads=1
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use ruststream_kinesis::prelude::*;
use ruststream_kinesis::{LeaseError, LeaseKey, LeaseState, LeaseStore};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "loyalty")]
struct Points {
    member: u64,
    earned: u64,
}

#[subscriber(KinesisStream::new("loyalty"))]
async fn credit(points: &Points) -> HandlerOutcome {
    tracing::info!(points.member, points.earned, "points credited");
    HandlerOutcome::ack()
}

// --8<-- [start:mount]
fn service(broker: KinesisBroker, checkpoints: Arc<Checkpoints>) -> RustStream {
    RustStream::new(AppInfo::new("loyalty", "0.1.0")).with_broker(
        broker.lease_store(checkpoints),
        |b| {
            b.include(credit);
        },
    )
}
// --8<-- [end:mount]

// --8<-- [start:key]
/// Checkpoints in a string-keyed map, for a single instance that owns every shard.
#[derive(Debug, Default)]
struct Checkpoints(Mutex<HashMap<String, String>>);

impl LeaseStore for Checkpoints {
    fn checkpoint<'a>(
        &'a self,
        key: &'a LeaseKey,
        _owner: &'a str,
        sequence: &'a str,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async move {
            self.0
                .lock()
                .map_err(|err| err.to_string())?
                .insert(key.to_string(), sequence.to_owned());
            Ok(true)
        })
    }

    fn read<'a>(&'a self, key: &'a LeaseKey) -> BoxFuture<'a, Result<LeaseState, LeaseError>> {
        Box::pin(async move {
            let checkpoint = self
                .0
                .lock()
                .map_err(|err| err.to_string())?
                .get(&key.to_string())
                .cloned();
            let mut state = LeaseState::default();
            state.checkpoint = checkpoint;
            Ok(state)
        })
    }

    // One instance owns every shard, so a lease is always granted and never lapses.
    fn acquire<'a>(
        &'a self,
        _key: &'a LeaseKey,
        _owner: &'a str,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async { Ok(true) })
    }

    fn renew<'a>(
        &'a self,
        _key: &'a LeaseKey,
        _owner: &'a str,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async { Ok(true) })
    }

    fn release<'a>(
        &'a self,
        _key: &'a LeaseKey,
        _owner: &'a str,
    ) -> BoxFuture<'a, Result<(), LeaseError>> {
        Box::pin(async { Ok(()) })
    }
}
// --8<-- [end:key]

#[ruststream::app]
fn app() -> impl App {
    let broker = KinesisBroker::new()
        .endpoint("http://localhost:4566")
        .test_credentials()
        .region("us-east-1");
    service(broker, Arc::new(Checkpoints::default()))
}

#[cfg(test)]
#[path = "support/kinesis_live.rs"]
mod support;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_store_keys_the_row_by_stream_and_shard() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = support::endpoint() else {
            return Ok(());
        };
        support::ensure_stream(&support::config(&endpoint).await, "loyalty", 1).await;
        let checkpoints = Arc::new(Checkpoints::default());

        let tb = TestApp::start_live(service(
            support::broker(&endpoint),
            Arc::clone(&checkpoints),
        ))
        .await?;
        tb.broker::<KinesisBroker>()
            .message(&Points {
                member: 42,
                earned: 120,
            })
            .publish()
            .await?;
        tb.broker::<KinesisBroker>()
            .subscriber("loyalty")
            .assert_called_once();
        tb.shutdown().await?;

        let keys: Vec<String> = checkpoints
            .0
            .lock()
            .map_err(|err| err.to_string())?
            .keys()
            .cloned()
            .collect();
        assert_eq!(keys, ["loyalty:shardId-000000000000"]);
        Ok(())
    }
}
