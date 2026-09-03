//! The crate prelude seen from a mount site: the two vocabularies a service imports, and which
//! one owns which name.

use ruststream_kinesis::prelude::*;

/// A handler body bounds an injected publisher with a broker capability trait, and that trait has
/// to survive the glob a routes file writes: the two vocabularies meet in one crate.
fn _a_publisher_bound_still_names_a_trait<T: Publisher>() {}

/// The `Seeker` trait, likewise: a handler calls `seek` on the handle the delivery context hands
/// it, so the glob has to bring the trait along.
fn _seeker_comes_with_the_glob<T: Seeker>() {}

#[test]
fn the_publish_policy_carries_the_uniform_mount_site_name() {
    // The uniform name is the point of the routes-side glob: `b.after_startup(Publish, ..)` reads
    // the same line whichever broker is underneath. Both positions - the type and the value -
    // have to resolve to the policy, which is what makes the alias load-bearing. (The policy
    // holds no options, so it is a unit struct and the `Publish::default()` spelling is what
    // clippy's `default_constructed_unit_structs` rejects.)
    let _: Publish = Publish;
}
