//! The service of `examples/kinesis_subscriptions_create.rs` written without the macros:
//! `create_if_missing` on the descriptor creates the stream when the stand lacks it.
//!
//! ```text
//! KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 \
//!     cargo test --example manual_kinesis_subscriptions_create --features testing -- --test-threads=1
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::{FixedName, MessageHeaders, NoHeaders, OutgoingDestination};
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Parcel {
    id: u64,
}

// `#[outgoing(name = "parcels")]` by hand: the type fixes its destination.
impl OutgoingDestination for Parcel {
    type Form = FixedName;

    const DESTINATION: &'static str = "parcels";
}

impl MessageHeaders for Parcel {
    type Contract = NoHeaders;
}

// --8<-- [start:create]
/// Tracks each parcel. On a stand that lacks the stream, the subscription creates it with two
/// shards before it opens.
struct Track;

impl Handle<Parcel> for Track {
    fn handle(
        &self,
        parcel: &Parcel,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(parcel.id, "parcel tracked");
        ready(Ok(()))
    }
}

fn service(broker: KinesisBroker) -> RustStream {
    RustStream::new(AppInfo::new("parcels", "0.1.0")).with_broker(broker, |b| {
        let parcels = KinesisStream::new("parcels").create_if_missing(2);
        b.include(subscriber(parcels, Track).build());
    })
}
// --8<-- [end:create]

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

    use ruststream::testing::{TestApp, TestError};

    use super::*;

    #[tokio::test]
    async fn zero_shards_stop_the_service_at_startup() {
        let app = RustStream::new(AppInfo::new("parcels", "0.1.0")).with_broker(
            KinesisBroker::new(),
            |b| {
                let shardless = KinesisStream::new("parcels").create_if_missing(0);
                b.include(subscriber(shardless, Track).build());
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
