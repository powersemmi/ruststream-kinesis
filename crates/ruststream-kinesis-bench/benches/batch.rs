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
//! Consuming in batches of 64: the batch size becomes the `GetRecords` limit, so each read
//! fetches one batch, the handler takes a slice, and the runtime settles every record in it.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, POLL_INTERVAL, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_kinesis::prelude::*;

#[subscriber(
    KinesisStream::new(common::stream()).poll_interval(POLL_INTERVAL),
    start_at(KinesisPosition::horizon())
)]
async fn consume(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    for order in orders {
        black_box((order.id, order.quantity));
        ctx.state().arrived();
    }
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, &[], |b| {
        b.include(consume.batch(nonzero!(64)));
    })
}

// Four runs put the longest one at 42,128 blocks every time. The limit this declares is 42,506:
// above that by more than a tenth of a percent, and below the 44,128 one more allocation per
// message would reach. A read, and the allocations it makes, come per batch rather than per
// delivery, so the rate is stated over a thousand deliveries.
#[library_benchmark(config = common::config_every(19_618, 1_000, 3_270))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = batch_group; benchmarks = service);
main!(library_benchmark_groups = batch_group);
