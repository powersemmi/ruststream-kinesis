//! `create_if_missing`: a subscription that creates its stream when the stand lacks it. The
//! scenarios run in process, and the creation itself against a stand.
//!
//! ```text
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
//!     cargo test --example kinesis_subscriptions_create --features testing -- --test-threads=1
//! ```

use ruststream_kinesis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "parcels")]
struct Parcel {
    id: u64,
}

// --8<-- [start:create]
/// Tracks each parcel. On a stand that lacks the stream, the subscription creates it with two
/// shards before it opens.
#[subscriber(KinesisStream::new("parcels").create_if_missing(2))]
async fn track(parcel: &Parcel) -> HandlerOutcome {
    tracing::info!(parcel.id, "parcel tracked");
    HandlerOutcome::ack()
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("parcels", "0.1.0")).with_broker(broker, |b| {
        b.include(track);
    })
}
// --8<-- [end:create]

#[ruststream::app]
fn app() -> impl App {
    service(
        KinesisBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
    )
}

#[cfg(test)]
#[path = "support/kinesis_live.rs"]
mod support;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{TestApp, TestError};

    use super::*;

    /// The same subscription, asking for a stream with no shards.
    #[subscriber(KinesisStream::new("parcels").create_if_missing(0))]
    async fn shardless(_parcel: &Parcel) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test]
    async fn zero_shards_stop_the_service_at_startup() {
        let app = RustStream::new(AppInfo::new("parcels", "0.1.0")).with_broker(
            KinesisBroker::new(),
            |b| {
                b.include(shardless);
            },
        );

        let refused = TestApp::start(app)
            .await
            .expect_err("a stream needs at least one shard");
        assert!(matches!(refused, TestError::Subscribe(_)), "{refused}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_missing_stream_is_created_with_the_shards_it_names() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = support::endpoint() else {
            return Ok(());
        };
        let config = support::config(&endpoint).await;
        support::delete_stream(&config, "parcels").await;

        let tb = TestApp::start_live(service(support::broker(&endpoint))).await?;
        assert_eq!(support::open_shards(&config, "parcels").await, 2);

        tb.broker::<KinesisBroker>()
            .message(&Parcel { id: 1 })
            .publish()
            .await?;
        tb.broker::<KinesisBroker>()
            .subscriber("parcels")
            .assert_called_once()
            .with(&Parcel { id: 1 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
