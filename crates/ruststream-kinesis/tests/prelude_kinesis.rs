//! The crate prelude seen from a mount site: the two vocabularies a service imports, and which
//! one owns which name.

use ruststream_kinesis::prelude::*;

/// A handler body bounds an injected publisher with a broker capability trait, and that trait has
/// to survive the glob a routes file writes: the two vocabularies meet in one crate.
fn _a_publisher_bound_still_names_a_trait<T: Publisher>() {}

/// The `Seeker` trait, likewise: a handler calls `seek` on the handle the delivery context hands
/// it, so the glob has to bring the trait along.
fn _seeker_comes_with_the_glob<T: Seeker>() {}

/// The mount-site publish settings are a trait too, and a routes file that names one has to get
/// it from the same glob as the policy it chains onto.
fn _publish_settings_come_with_the_glob<T: KinesisPublishSettings>() {}

/// The per-message steps are the one part of this glob a handler body imports, so the trait has
/// to arrive through it.
fn _publish_steps_come_with_the_glob<T: KinesisPublishSteps>() {}

/// And so does the options type, because that same body writes it into its own bound.
fn _a_body_bounds_its_slot_on_the_options_type<T: Publisher<Options = KinesisPublishOptions>>() {}

#[test]
fn the_publish_policy_carries_the_uniform_mount_site_name() {
    // The uniform name is the point of the routes-side glob: `b.after_startup(Publish, ..)` reads
    // the same line whichever broker is underneath. Both positions - the type and the value -
    // have to resolve to the policy, which is what makes the alias load-bearing.
    let _: Publish = Publish::default().partition_key("tenant-acme");
}
