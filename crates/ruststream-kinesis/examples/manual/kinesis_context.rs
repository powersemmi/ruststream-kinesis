//! The service of `examples/kinesis_context.rs` written without the macros: a body typed over
//! `KinesisContext` reads the record's position off its context by the `Position` key.
//!
//! ```text
//! cargo test --example manual_kinesis_context --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The slot the audit trail is written through.
#[derive(OutSlot)]
#[publishes(Located)]
struct Trail;

// --8<-- [start:context]
/// Writes where each order sits in the stream to the audit trail.
struct Audit;

impl<Entry> Handle<Order, (), Outs<(Entry,)>, KinesisContext> for Audit
where
    Entry: OutEntry<Trail, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        ctx: &mut Context<'_, KinesisContext>,
    ) -> Result<(), HandlerOutcome> {
        // A delivered record's position is always one record of one shard.
        let KinesisPosition::Sequence { shard, sequence } = ctx.context(Position) else {
            return Err(HandlerOutcome::drop());
        };
        tracing::info!(order.id, shard, sequence, "order located");
        let located = Located {
            order: order.id,
            shard,
            sequence,
        };
        outs.get(Trail)
            .message(&located)
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:context]

fn app() -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(subscriber(KinesisStream::new("orders"), Audit).build())
            .out(Trail, Publish::default())
            .build();
    })
}

fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
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
