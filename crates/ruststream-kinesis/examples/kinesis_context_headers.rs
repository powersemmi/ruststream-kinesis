//! The delivery headers: every record arrives with its sequence number, its shard and its
//! partition key as headers, and `workers(n, by_key)` runs the records of one key in order.
//!
//! ```text
//! cargo test --example kinesis_context_headers --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

/// The slot the bills leave through.
#[derive(OutSlot)]
#[publishes(Bill)]
struct Bills;

// --8<-- [start:headers]
use ruststream_kinesis::{PARTITION_KEY_HEADER, SEQUENCE_HEADER, SHARD_HEADER};

/// Bills each meter reading. The meter is the record's partition key, so the readings of one
/// meter run one after another on four lanes, and in the order the stream holds them.
#[subscriber(KinesisStream::new("meters"), workers(4, by_key))]
async fn bill(
    reading: &Reading,
    ctx: &mut Context<'_>,
    Out(bills): Out<impl Publisher, Bills>,
) -> HandlerOutcome {
    let headers = ctx.headers();
    let (Some(meter), Some(shard), Some(sequence)) = (
        headers.get_str(PARTITION_KEY_HEADER),
        headers.get_str(SHARD_HEADER),
        headers.get_str(SEQUENCE_HEADER),
    ) else {
        return HandlerOutcome::drop();
    };
    tracing::info!(meter, shard, sequence, reading.kwh, "reading billed");
    let charge = Bill {
        meter: meter.to_owned(),
        kwh: reading.kwh,
    };
    if bills.message(&charge).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:headers]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(bill).out(Bills, Publish::default()).build();
    })
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "meters")]
struct Reading {
    kwh: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "bills")]
struct Bill {
    meter: String,
    kwh: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::PARTITION_KEY_HEADER;
    use ruststream_kinesis::prelude::*;

    use super::{Bill, Reading, app};

    #[tokio::test]
    async fn a_reading_is_billed_to_its_meter() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let mut keyed = HeaderMap::new();
        keyed.insert(PARTITION_KEY_HEADER, "meter-12");
        tb.broker::<KinesisBroker>()
            .message(&Reading { kwh: 40 })
            .with_headers(keyed)
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .published::<Bill>("bills")
            .assert_called_once()
            .with(&Bill {
                meter: "meter-12".to_owned(),
                kwh: 40,
            });
        tb.shutdown().await?;
        Ok(())
    }
}
