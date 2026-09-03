//! The crate prelude seen from a consumer's position: what one glob has to leave intact.

use ruststream_kinesis::prelude::*;

/// The framework's `Publish` slot trait must survive the glob. Re-exporting a broker policy under
/// that bare name would shadow it - an explicit re-export beats the framework's own glob - and
/// this bound would stop naming a trait at all.
fn _publish_stays_the_frameworks_slot_trait<T: Publish>() {}

/// The `Seeker` trait, likewise: a handler calls `seek` on the handle the delivery context hands
/// it, so the glob has to bring the trait along.
fn _seeker_comes_with_the_glob<T: Seeker>() {}

#[test]
fn the_publish_policy_keeps_its_prefixed_name() {
    // The policy is pure declaration, so naming it is the whole check; it coexists with the
    // framework's `Publish` trait above precisely because it is not called `Publish`.
    let policy = KinesisPublish;
    assert_eq!(format!("{policy:?}"), "KinesisPublish");
}
