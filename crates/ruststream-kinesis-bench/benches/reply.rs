// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! Replying: the handler returns a value, the runtime encodes it and hands it to the publisher
//! this crate's default policy pairs, which resolves the record's partition key and sends it with
//! `PutRecord` to the stream the reply type declares.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, POLL_INTERVAL, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_kinesis::prelude::*;
use serde::Serialize;

/// A reply with a stream of its own: the mount site adds nothing to it.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

/// The stream [`Confirmation`] names, created on the stand before the service starts: a
/// publisher sends to a stream, it does not create one.
const CONFIRMATIONS: &[&str] = &["confirmations"];

#[subscriber(
    KinesisStream::new(common::stream()).poll_interval(POLL_INTERVAL),
    start_at(KinesisPosition::horizon()),
    publish
)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

fn app(messages: usize) -> Pending {
    common::pending(messages, CONFIRMATIONS, |b| {
        b.include(confirm);
    })
}

// Four runs put the longest one at 839,850 to 839,865 blocks. The limit this declares is 840,749:
// above the highest by more than a tenth of a percent, and below the 841,865 one more allocation
// per message would reach. The rate is rounded up from the 417.99 measured to reach that margin,
// and stated over a thousand deliveries.
#[library_benchmark(config = common::config_every(418_600, 1_000, 3_549))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply_group; benchmarks = service);
main!(library_benchmark_groups = reply_group);
