//! What one publish through the in-process transport costs.
//!
//! The publishers declare `Take`, so the framework hands them the header map the publish filled.
//! Address equality proves nothing about a map: `Bytes` keeps the data pointer across a clone, so
//! the proof is the number of allocations the publish makes.
#![cfg(feature = "testing")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ruststream::{Broker, BytesMut, HeaderMap, OutgoingFor, OutgoingMessage, Publisher, Take};
use ruststream_kinesis::testing::KinesisTestBroker;

/// Counts this thread's allocations, so the cost of one publish can be read off directly.
/// Thread-local rather than global: the other tests of this binary run beside it and their
/// allocations are none of this measurement's business.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        // SAFETY: the layout is the caller's, forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What this thread has allocated so far.
fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

/// A message with two headers, in the form a taking publisher is handed.
fn two_header_message() -> OutgoingFor<'static, Take> {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    OutgoingMessage::produced("orders", BytesMut::from(&br#"{"id":1}"#[..])).with_headers(headers)
}

/// The publish hands the router the map it was given rather than a copy of it. What is left in
/// the count is the partition key the stand-in stamps on the delivery, the stream name the log is
/// keyed by, and the delivery the router builds: its own bookkeeping, not a transport cost.
/// Reintroducing the clone puts the count back up.
#[tokio::test]
async fn a_publish_with_no_subscriber_spends_nothing_on_its_header_map() {
    let broker = KinesisTestBroker::new().connect().await.expect("connect");
    let publisher = broker.publisher();

    // The log's own growth is not the subject: one publish outside the counted region leaves it
    // with room for the next. The message the region measures is built outside it too.
    publisher
        .publish(two_header_message(), None)
        .await
        .expect("publish failed");
    let msg = two_header_message();

    let before = allocations();
    publisher.publish(msg, None).await.expect("publish failed");
    let spent = allocations() - before;

    assert_eq!(
        spent, 8,
        "the partition key, the log's key and the delivery the router builds, and nothing for \
         the header map the publish handed over"
    );
}
