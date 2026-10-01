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
//! Consuming a small JSON body: the shard reader fetches the records with `GetRecords`, the
//! dispatcher decodes each into a struct, the handler reads a field, and the runtime acks it,
//! which checkpoints the shard in the default lease store.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, POLL_INTERVAL, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_kinesis::prelude::*;

#[subscriber(
    KinesisStream::new(common::stream()).poll_interval(POLL_INTERVAL),
    start_at(KinesisPosition::horizon())
)]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, &[], |b| {
        b.include(consume);
    })
}

// Five runs put the longest one at 29,921 to 29,925 blocks. The limit this declares is 30,320:
// above the highest by more than a tenth of a percent, and below the 31,925 one more allocation
// per message would reach. A read's allocations are shared by the records it returns, so the rate
// is stated over a thousand deliveries.
#[library_benchmark(config = common::config_every(13_558, 1_000, 3_204))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = consume_group; benchmarks = service);
main!(library_benchmark_groups = consume_group);
