//! Repositioning the in-process transport: the shared seek surface of one subscription, and the
//! handle a delivery hands to a handler.
//!
//! The stand-in keeps a retained log per stream, so a position is an index into that log and a
//! seek is a real re-read of it - the same shape the service gives a shard iterator. Applying a
//! seek is a handoff rather than an in-place mutation: the seeker records the target and wakes
//! the stream, and the subscriber swaps its queue inside its own poll, where the mutable borrow
//! of the receiver is available.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::task::AtomicWaker;

use crate::error::KinesisError;
use crate::message::KinesisPosition;
use crate::testing::broker::TestState;
use crate::testing::router::Plan;

/// The shard every in-process subscription reports.
///
/// The stand-in routes one retained log per stream, which is one shard's worth of ordering, so
/// it names that shard the way the service names a first shard. Captured positions carry it, and
/// a [`KinesisPosition::Sequence`] naming any other shard is refused rather than silently
/// applied to this one.
///
/// # Examples
///
/// ```
/// use ruststream_kinesis::testing::IN_PROCESS_SHARD;
///
/// assert!(IN_PROCESS_SHARD.starts_with("shardId-"));
/// ```
pub const IN_PROCESS_SHARD: &str = "shardId-000000000000";

/// Renders a log index the way the service renders a sequence number: a wide zero-padded
/// decimal, so the numeric and the lexicographic order agree.
pub(crate) fn sequence_of(index: usize) -> String {
    format!("{index:020}")
}

/// Shared between one subscription's polling side and the seekers minted off it.
#[derive(Default)]
pub(crate) struct SeekControl {
    /// The reposition the seeker resolved, taken by the subscriber inside its next poll.
    pending: Mutex<Option<Plan>>,
    /// Deliveries stamped below this index are stale pre-seek copies (a requeue that raced the
    /// seek) and are dropped by the polling side.
    watermark: AtomicUsize,
    /// Wakes the subscription's stream task after `pending` is set.
    pub(crate) waker: AtomicWaker,
}

impl SeekControl {
    /// The stale-delivery cutoff, read on the polling side for every delivery.
    ///
    /// Acquire pairs with the Release store in [`SeekControl::install`]: a poll that observes a
    /// delivery enqueued after a seek also observes that seek's watermark.
    pub(crate) fn watermark(&self) -> usize {
        self.watermark.load(Ordering::Acquire)
    }

    /// Takes the reposition the seeker left, if any.
    pub(crate) fn take_pending(&self) -> Option<Plan> {
        self.pending
            .lock()
            .expect("kinesis test seek mutex poisoned")
            .take()
    }

    /// Records a reposition and wakes the subscription.
    fn install(&self, plan: Plan) {
        // Watermark first (Release, paired with the Acquire load in the delivery filter), then
        // the pending plan: a poll that takes the plan must see its watermark.
        self.watermark.store(plan.target, Ordering::Release);
        *self
            .pending
            .lock()
            .expect("kinesis test seek mutex poisoned") = Some(plan);
        self.waker.wake();
    }
}

impl std::fmt::Debug for SeekControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeekControl")
            .field("watermark", &self.watermark())
            .finish_non_exhaustive()
    }
}

/// Repositions one in-process subscription over its stream's retained log.
///
/// The in-process arm of [`KinesisSeeker`](crate::KinesisSeeker): a handler that reads
/// [`SeekHandle`](crate::SeekHandle) off its delivery context gets one of these when it runs on
/// [`KinesisTestBroker`](crate::testing::KinesisTestBroker), and the seek really re-reads the log.
#[derive(Clone)]
pub(crate) struct LogSeeker {
    state: Arc<TestState>,
    address: Arc<str>,
    control: Arc<SeekControl>,
}

impl std::fmt::Debug for LogSeeker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogSeeker")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl LogSeeker {
    pub(crate) fn new(state: Arc<TestState>, address: Arc<str>, control: Arc<SeekControl>) -> Self {
        Self {
            state,
            address,
            control,
        }
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
        self.state.ensure_open()?;
        let plan = self.state.router.plan(&self.address, to)?;
        // The replay is counted in flight before this call returns, not when the subscription
        // applies it: the seek is the moment a test can observe, and a quiescence wait that ran
        // between the two would otherwise find an empty in-flight count and call the reaction
        // finished while a whole replay was still pending.
        if let Some(coordinator) = self.state.coordinator() {
            for _ in 0..plan.count {
                coordinator.enqueued();
            }
        }
        self.control.install(plan);
        Ok(())
    }
}
