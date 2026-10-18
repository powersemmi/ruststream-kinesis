//! The service of `examples/kinesis_context_headers.rs` written without the macros: the body
//! reads the three delivery headers off its context, and `.workers_by_key(n)` runs the records
//! of one key in order.
//!
//! ```text
//! cargo test --example manual_kinesis_context_headers --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_kinesis::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The slot the bills leave through.
#[derive(OutSlot)]
#[publishes(Bill)]
struct Bills;

// --8<-- [start:headers]
use ruststream_kinesis::{PARTITION_KEY_HEADER, SEQUENCE_HEADER, SHARD_HEADER};

/// Bills each meter reading. The meter is the record's partition key, so the readings of one
/// meter run one after another on four lanes, and in the order the stream holds them.
struct Charge;

impl<Entry> Handle<Reading, (), Outs<(Entry,)>> for Charge
where
    Entry: OutEntry<Bills, Wire: Publisher>,
{
    async fn handle(
        &self,
        reading: &Reading,
        outs: &Outs<(Entry,)>,
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let headers = ctx.headers();
        let (Some(meter), Some(shard), Some(sequence)) = (
            headers.get_str(PARTITION_KEY_HEADER),
            headers.get_str(SHARD_HEADER),
            headers.get_str(SEQUENCE_HEADER),
        ) else {
            return Err(HandlerOutcome::drop());
        };
        tracing::info!(meter, shard, sequence, reading.kwh, "reading billed");
        let bill = Bill {
            meter: meter.to_owned(),
            kwh: reading.kwh,
        };
        outs.get(Bills)
            .message(&bill)
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(
            subscriber(KinesisStream::new("meters"), Charge)
                .workers_by_key(nonzero!(4))
                .build(),
        )
        .out(Bills, Publish::default())
        .build();
    })
}
// --8<-- [end:headers]

fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
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
