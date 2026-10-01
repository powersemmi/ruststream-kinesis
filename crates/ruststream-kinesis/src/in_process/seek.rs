//! Repositioning the in-process transport: the shared seek surface of one subscription, and the
//! handle a delivery hands to a handler.
//!
//! A position is an index into the stream's retained log, and a reposition is a real re-read of
//! it - the shape the service gives a shard iterator. Applying one is a handoff rather than an
//! in-place mutation: the seeker records the target and wakes the subscription, and the
//! subscription swaps its queue inside its own poll, where the mutable borrow of the receiver is
//! available.
//!
//! A seek, from `start_at(..)` or a handler's seek handle, is what repositions a subscription.

use std::sync::{Arc, Mutex, MutexGuard};

use futures::task::AtomicWaker;
use ruststream::testing::Coordinator;

use super::Bus;
use super::log::Plan;
use crate::error::KinesisError;
use crate::message::KinesisPosition;

/// The shard every in-process stream has.
///
/// A stream here is one retained log, which is one shard's worth of ordering, so it names that
/// shard the way the service names a stream's first shard. Captured positions carry it, and a
/// [`KinesisPosition::Sequence`] naming any other shard is refused rather than applied to this
/// one.
pub(crate) const IN_PROCESS_SHARD: &str = "shardId-000000000000";

/// The digits a sequence number starts with; the rest is the record's index in the log.
const SEQUENCE_PREFIX: &str = "49";

/// Renders a log index the way the service renders a sequence number: a 56-digit decimal, too
/// wide for any integer type, so a handler that parses one as a number fails here as it fails
/// against the service. The width is fixed, so the numeric and the lexicographic order agree.
pub(crate) fn sequence_of(index: usize) -> String {
    format!("{SEQUENCE_PREFIX}{index:054}")
}

/// The log index a sequence number this transport issued stands for.
pub(crate) fn index_of(sequence: &str) -> Option<usize> {
    let digits = sequence.strip_prefix(SEQUENCE_PREFIX)?;
    if digits.len() != 54 {
        return None;
    }
    digits.parse().ok()
}

/// Shared between one subscription's polling side and the seekers minted off it.
#[derive(Default)]
pub(crate) struct SeekControl {
    /// The reposition resolved and not yet applied, taken by the subscription inside its next
    /// poll.
    pending: Mutex<Option<Plan>>,
    /// Wakes the subscription's stream task after `pending` is set.
    pub(crate) waker: AtomicWaker,
}

impl SeekControl {
    fn lock(&self) -> MutexGuard<'_, Option<Plan>> {
        self.pending
            .lock()
            .expect("kinesis in-process seek mutex poisoned")
    }

    /// Takes the reposition left for the subscription, if any.
    pub(crate) fn take_pending(&self) -> Option<Plan> {
        self.lock().take()
    }

    /// Records a seek and wakes the subscription; it replaces any reposition not yet applied.
    ///
    /// The replay this plan promises is counted in flight here, because the seek is the moment a
    /// test can observe: a quiescence wait between the count and the swap would otherwise find an
    /// empty in-flight total and call the reaction finished while a whole replay was pending.
    ///
    /// A plan this one displaces is a plan the subscription never applied - taking it is what
    /// clears the slot - so its own count is released again. Counting the new plan before
    /// releasing the displaced one keeps the total off zero in between.
    fn install_seek(&self, plan: Plan, coordinator: Option<&Coordinator>) {
        count(coordinator, plan.count);
        let displaced = self.lock().replace(plan);
        // Derived from the swap rather than from a separate read: the swap is what decides which
        // of two concurrent seeks displaced the other, so exactly one of them releases a count.
        release(
            coordinator,
            displaced.map_or(0, |displaced| displaced.count),
        );
        self.waker.wake();
    }
}

fn count(coordinator: Option<&Coordinator>, deliveries: usize) {
    if let Some(coordinator) = coordinator {
        for _ in 0..deliveries {
            coordinator.enqueued();
        }
    }
}

fn release(coordinator: Option<&Coordinator>, deliveries: usize) {
    if let Some(coordinator) = coordinator {
        for _ in 0..deliveries {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for SeekControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeekControl").finish_non_exhaustive()
    }
}

/// Repositions one in-process subscription over its stream's retained log.
///
/// The in-process arm of [`KinesisSeeker`](crate::KinesisSeeker): a handler that reads
/// [`SeekHandle`](crate::SeekHandle) off its delivery context gets one of these when the harness
/// connected its broker in process, and the seek really re-reads the log.
#[derive(Clone)]
pub(crate) struct LogSeeker {
    bus: Arc<Bus>,
    stream: Arc<str>,
    control: Arc<SeekControl>,
}

impl std::fmt::Debug for LogSeeker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogSeeker")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl LogSeeker {
    pub(crate) const fn new(bus: Arc<Bus>, stream: Arc<str>, control: Arc<SeekControl>) -> Self {
        Self {
            bus,
            stream,
            control,
        }
    }

    /// The subscription's shared seek surface.
    pub(crate) fn control(&self) -> &SeekControl {
        &self.control
    }

    /// Resolves `to` against the retained log and hands the reposition to the subscription.
    ///
    /// Synchronous because nothing here leaves the process; the async seam is
    /// [`Seeker::seek`](ruststream::Seeker::seek) itself.
    ///
    /// # Errors
    ///
    /// Returns [`KinesisError::NotConnected`] through a handle that outlived `shutdown`, and
    /// [`KinesisError::Read`] for a position this transport cannot address.
    pub(crate) fn seek(&self, to: &KinesisPosition) -> Result<(), KinesisError> {
        self.bus.ensure_open()?;
        let plan = self.bus.log().plan(&self.stream, to)?;
        self.control.install_seek(plan, self.bus.coordinator());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sequence number reads back as the index it was rendered from, and one the transport
    /// never issued reads back as nothing.
    #[test]
    fn a_sequence_number_round_trips_to_its_index() {
        let sequence = sequence_of(42);
        assert_eq!(sequence.len(), 56);
        assert_eq!(index_of(&sequence), Some(42));
        assert_eq!(index_of("42"), None);
        assert_eq!(index_of("not-a-sequence-number"), None);
    }

    /// The width is fixed, so a later record sorts after an earlier one as text too.
    #[test]
    fn sequence_numbers_sort_in_log_order() {
        assert!(sequence_of(9) < sequence_of(10));
    }
}
