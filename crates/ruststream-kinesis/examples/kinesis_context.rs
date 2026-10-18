//! The delivery context: a handler reads where its record sits in the stream through the
//! `Position` key, and writes it to the audit trail.
//!
//! ```text
//! cargo test --example kinesis_context --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

/// The slot the audit trail is written through.
#[derive(OutSlot)]
#[publishes(Located)]
struct Trail;

// --8<-- [start:context]
/// Writes where each order sits in the stream to the audit trail.
#[subscriber(KinesisStream::new("orders"))]
async fn audit(
    order: &Order,
    Ctx(at): Ctx<Position>,
    Out(trail): Out<impl Publisher, Trail>,
) -> HandlerOutcome {
    // A delivered record's position is always one record of one shard.
    let KinesisPosition::Sequence { shard, sequence } = at else {
        return HandlerOutcome::drop();
    };
    tracing::info!(order.id, shard, sequence, "order located");
    let located = Located {
        order: order.id,
        shard,
        sequence,
    };
    if trail.message(&located).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:context]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(audit).out(Trail, Publish::default()).build();
    })
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

/// Where an order sits: the shard it was read from and its sequence number there.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "audit-trail")]
struct Located {
    order: u64,
    shard: String,
    sequence: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Located, Order, app};

    #[tokio::test]
    async fn each_order_is_located_in_the_stream() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for id in [1, 2] {
            tb.broker::<KinesisBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }

        let located = tb
            .broker::<KinesisBroker>()
            .published::<Located>("audit-trail")
            .assert_called(2)
            .decoded();
        let shards: Vec<&str> = located.iter().map(|at| at.shard.as_str()).collect();
        assert_eq!(shards, ["shardId-000000000000", "shardId-000000000000"]);
        // Sequence numbers grow along a shard.
        assert!(located[0].sequence < located[1].sequence, "{located:?}");
        tb.shutdown().await?;
        Ok(())
    }
}
